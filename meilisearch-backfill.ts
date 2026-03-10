/**
 * Meilisearch backfill script.
 *
 * Configures index settings (filterable/sortable attributes, synonyms, ranking
 * rules, typo tolerance) then reads all records from Postgres and bulk-imports
 * them into Meilisearch.
 *
 * Intended to run as an ephemeral Railway service on the internal network.
 *
 * Env vars:
 *   DATABASE_URL          — Postgres connection string
 *   MEILISEARCH_URL       — e.g. http://meilisearch.railway.internal:7700
 *   MEILISEARCH_API_KEY   — Admin API key
 */

import postgres from "postgres";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const DATABASE_URL = env("DATABASE_URL");
const MEILI_URL = env("MEILISEARCH_URL");
const MEILI_KEY = env("MEILISEARCH_API_KEY");
const INDEX = "records";
const BATCH_SIZE = 500;

function env(name: string): string {
  const v = process.env[name];
  if (!v) {
    console.error(`Missing required env var: ${name}`);
    process.exit(1);
  }
  return v;
}

// ---------------------------------------------------------------------------
// Meilisearch helpers
// ---------------------------------------------------------------------------

const meiliHeaders = {
  Authorization: `Bearer ${MEILI_KEY}`,
  "Content-Type": "application/json",
};

async function meili(
  path: string,
  method: string = "GET",
  body?: unknown
): Promise<unknown> {
  const res = await fetch(`${MEILI_URL}${path}`, {
    method,
    headers: meiliHeaders,
    body: body != null ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Meilisearch ${method} ${path} → ${res.status}: ${text}`);
  }
  return res.json();
}

async function waitForTask(taskUid: number): Promise<void> {
  while (true) {
    const task = (await meili(`/tasks/${taskUid}`)) as {
      status: string;
      error?: { message: string };
    };
    if (task.status === "succeeded") return;
    if (task.status === "failed") {
      throw new Error(
        `Meilisearch task ${taskUid} failed: ${task.error?.message}`
      );
    }
    await new Promise((r) => setTimeout(r, 250));
  }
}

async function meiliTask(
  path: string,
  method: string,
  body?: unknown
): Promise<void> {
  const result = (await meili(path, method, body)) as { taskUid: number };
  await waitForTask(result.taskUid);
}

// ---------------------------------------------------------------------------
// Index settings
// ---------------------------------------------------------------------------

async function configureIndex(): Promise<void> {
  console.log("Configuring index settings...");

  // Create index if it doesn't exist (ignore "already exists" errors)
  try {
    const result = (await meili("/indexes", "POST", {
      uid: INDEX,
      primaryKey: "id",
    })) as { taskUid: number };
    await waitForTask(result.taskUid);
  } catch (err) {
    if (String(err).includes("already exists")) {
      console.log("  Index already exists, continuing...");
    } else {
      throw err;
    }
  }

  // Filterable attributes — enables faceted search and filter queries
  await meiliTask(`/indexes/${INDEX}/settings/filterable-attributes`, "PUT", [
    "type",
    "profileType",
    "did",
    "genres",
    "themes",
    "modes",
    "playerPerspectives",
    "applicationType",
    "category",
    "collectionType",
    "country",
    "status",
  ]);

  // Sortable attributes
  await meiliTask(`/indexes/${INDEX}/settings/sortable-attributes`, "PUT", [
    "name",
    "displayName",
    "publishedAt",
    "firstReleaseDate",
  ]);

  // Searchable attributes — priority order (most important first)
  await meiliTask(`/indexes/${INDEX}/settings/searchable-attributes`, "PUT", [
    "name",
    "displayName",
    "alternativeNames",
    "alternativeName",
    "abbreviation",
    "summary",
    "description",
    "keywords",
    "storyline",
  ]);

  // Synonyms — common gaming abbreviations
  await meiliTask(`/indexes/${INDEX}/settings/synonyms`, "PUT", {
    rpg: ["role-playing game", "role playing game"],
    "role-playing game": ["rpg"],
    fps: ["first-person shooter", "first person shooter"],
    "first-person shooter": ["fps"],
    rts: ["real-time strategy", "real time strategy"],
    "real-time strategy": ["rts"],
    mmo: ["massively multiplayer online"],
    mmorpg: ["massively multiplayer online role-playing game"],
    dlc: ["downloadable content"],
    "downloadable content": ["dlc"],
    indie: ["independent"],
    co_op: ["cooperative", "coop"],
    coop: ["cooperative", "co-op"],
    pvp: ["player versus player", "player vs player"],
    pve: ["player versus environment", "player vs environment"],
    vr: ["virtual reality"],
    "virtual reality": ["vr"],
    ar: ["augmented reality"],
    "augmented reality": ["ar"],
    jrpg: ["japanese role-playing game", "japanese rpg"],
    arpg: ["action role-playing game", "action rpg"],
    crpg: ["computer role-playing game", "computer rpg"],
    "4x": ["explore expand exploit exterminate"],
    roguelike: ["rogue-like"],
    roguelite: ["rogue-lite"],
    metroidvania: ["metroid-vania"],
    "beat em up": ["beat-em-up", "brawler"],
    shmup: ["shoot em up", "shoot-em-up"],
    sim: ["simulator", "simulation"],
    platformer: ["platform"],
  });

  // Ranking rules — boost exact name matches, then prefer newer games
  await meiliTask(`/indexes/${INDEX}/settings/ranking-rules`, "PUT", [
    "words",
    "typo",
    "proximity",
    "attribute",
    "sort",
    "exactness",
    "firstReleaseDate:desc",
  ]);

  // Typo tolerance — tuned for game names
  await meiliTask(`/indexes/${INDEX}/settings/typo-tolerance`, "PATCH", {
    enabled: true,
    minWordSizeForTypos: {
      oneTypo: 4,
      twoTypos: 8,
    },
    disableOnAttributes: ["genres", "themes", "modes", "applicationType"],
  });

  console.log("Index settings configured.");
}

// ---------------------------------------------------------------------------
// Record mapping — mirrors the Lua index hooks
// ---------------------------------------------------------------------------

type Record = { [key: string]: unknown };

/** Base64url-encode an AT URI for use as a Meilisearch document ID. */
function toDocId(uri: string): string {
  return Buffer.from(uri).toString("base64url");
}

/**
 * Parse a releasedAt string into a numeric YYYYMMDD value for sorting.
 * Handles: "YYYY-MM-DD", "YYYY-MM", "YYYY-Q1".."YYYY-Q4", "YYYY".
 * Returns undefined for unparseable values.
 */
const Q_MONTH: { [key: string]: number } = { Q1: 1, Q2: 4, Q3: 7, Q4: 10 };

function parseReleaseDate(s: unknown): number | undefined {
  if (typeof s !== "string") return undefined;

  // YYYY-Qn
  const qMatch = s.match(/^(\d{4})-?(Q[1-4])$/);
  if (qMatch) {
    return Number(qMatch[1]) * 10000 + Q_MONTH[qMatch[2]] * 100 + 1;
  }

  // YYYY-MM-DD
  const fullMatch = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (fullMatch) {
    return Number(fullMatch[1]) * 10000 + Number(fullMatch[2]) * 100 + Number(fullMatch[3]);
  }

  // YYYY-MM
  const monthMatch = s.match(/^(\d{4})-(\d{2})$/);
  if (monthMatch) {
    return Number(monthMatch[1]) * 10000 + Number(monthMatch[2]) * 100 + 1;
  }

  // YYYY
  const yearMatch = s.match(/^(\d{4})$/);
  if (yearMatch) {
    return Number(yearMatch[1]) * 10000 + 101;
  }

  return undefined;
}

// Sentinel values for sorting with firstReleaseDate:desc.
// Neutral sits among current-era releases; cancelled sinks to the bottom.
function neutralDate(): number {
  const now = new Date();
  return now.getFullYear() * 10000 + (now.getMonth() + 1) * 100 + now.getDate();
}
const CANCELLED_DATE = 10000101; // far past — deprioritised

const CANCELLED_STATUSES = new Set(["cancelled", "offline"]);

/**
 * Find the earliest release date across all platforms/regions.
 *
 * - Concrete dates (past or future) are parsed as-is — future dates naturally
 *   rank high with desc ordering
 * - If all release entries are cancelled → CANCELLED_DATE (deprioritised)
 * - TBD / no parseable date → neutral (today's date, ranks among current releases)
 */
function getFirstReleaseDate(releases: unknown): number {
  if (!Array.isArray(releases) || releases.length === 0) return neutralDate();

  let earliest: number | undefined;
  let hasAnyEntry = false;
  let allCancelled = true;

  for (const rel of releases) {
    if (!rel || typeof rel !== "object") continue;
    const releaseDates = (rel as Record).releaseDates;
    if (!Array.isArray(releaseDates)) continue;
    for (const rd of releaseDates) {
      if (!rd || typeof rd !== "object") continue;
      hasAnyEntry = true;

      const status = (rd as Record).status as string | undefined;
      if (!status || !CANCELLED_STATUSES.has(status)) {
        allCancelled = false;
      }

      const val = parseReleaseDate((rd as Record).releasedAt);
      if (val !== undefined && (earliest === undefined || val < earliest)) {
        earliest = val;
      }
    }
  }

  if (hasAnyEntry && allCancelled) return CANCELLED_DATE;
  return earliest ?? neutralDate();
}

const COLLECTIONS: { [collection: string]: string } = {
  "games.gamesgamesgamesgames.game": "game",
  "games.gamesgamesgamesgames.platform": "platform",
  "games.gamesgamesgamesgames.collection": "collection",
  "games.gamesgamesgamesgames.engine": "engine",
  "games.gamesgamesgamesgames.actor.profile": "actorProfile",
  "games.gamesgamesgamesgames.org.profile": "orgProfile",
};

function mapRecord(
  uri: string,
  did: string,
  collection: string,
  record: Record,
  slugsByRef: Map<string, string>
): Record | null {
  const type = COLLECTIONS[collection];
  if (!type) return null;

  const slug = slugsByRef.get(uri);

  switch (type) {
    case "game": {
      const altNames: string[] = [];
      if (Array.isArray(record.alternativeNames)) {
        for (const an of record.alternativeNames) {
          if (an && typeof an === "object" && (an as Record).name) {
            altNames.push((an as Record).name as string);
          }
        }
      }
      return {
        id: toDocId(uri),
        type: "game",
        did,
        uri,
        name: record.name,
        summary: record.summary,
        storyline: record.storyline,
        keywords: record.keywords,
        genres: record.genres,
        modes: record.modes,
        themes: record.themes,
        playerPerspectives: record.playerPerspectives,
        alternativeNames: altNames.length > 0 ? altNames : undefined,
        multiplayerModes: record.multiplayerModes,
        applicationType: record.applicationType,
        publishedAt: record.publishedAt,
        firstReleaseDate: getFirstReleaseDate(record.releases),
        media: record.media,
        ...(slug ? { slug } : {}),
      };
    }
    case "platform":
      return {
        id: toDocId(uri),
        type: "platform",
        did,
        uri,
        name: record.name,
        abbreviation: record.abbreviation,
        alternativeName: record.alternativeName,
        description: record.description,
        category: record.category,
        ...(slug ? { slug } : {}),
      };
    case "collection":
      return {
        id: toDocId(uri),
        type: "collection",
        did,
        uri,
        name: record.name,
        description: record.description,
        collectionType: record.type,
        ...(slug ? { slug } : {}),
      };
    case "engine":
      return {
        id: toDocId(uri),
        type: "engine",
        did,
        uri,
        name: record.name,
        description: record.description,
        ...(slug ? { slug } : {}),
      };
    case "actorProfile":
      return {
        id: toDocId(uri),
        type: "profile",
        profileType: "actor",
        did,
        uri,
        displayName: record.displayName,
        description: record.description,
        pronouns: record.pronouns,
        avatar: record.avatar,
      };
    case "orgProfile":
      return {
        id: toDocId(uri),
        type: "profile",
        profileType: "org",
        did,
        uri,
        displayName: record.displayName,
        description: record.description,
        country: record.country,
        status: record.status,
        avatar: record.avatar,
      };
    default:
      return null;
  }
}

// ---------------------------------------------------------------------------
// Backfill
// ---------------------------------------------------------------------------

async function backfill(): Promise<void> {
  const sql = postgres(DATABASE_URL);

  // Load all slugs from the slugs table into a map: uri → slug
  console.log("Loading slugs...");
  const slugRows = await sql`
    SELECT uri, slug FROM slugs
  `;
  const slugsByRef = new Map<string, string>();
  for (const row of slugRows) {
    slugsByRef.set(row.uri, row.slug);
  }
  console.log(`Loaded ${slugsByRef.size} slugs.`);

  // Indexed collections
  const collections = Object.keys(COLLECTIONS);

  let totalIndexed = 0;

  for (const collection of collections) {
    console.log(`\nBackfilling ${collection}...`);
    let offset = 0;
    let collectionCount = 0;

    while (true) {
      const rows = await sql`
        SELECT uri, did, record
        FROM records
        WHERE collection = ${collection}
        ORDER BY uri
        LIMIT ${BATCH_SIZE}
        OFFSET ${offset}
      `;

      if (rows.length === 0) break;

      const docs: Record[] = [];
      for (const row of rows) {
        const doc = mapRecord(
          row.uri,
          row.did,
          collection,
          row.record as Record,
          slugsByRef
        );
        if (doc) docs.push(doc);
      }

      if (docs.length > 0) {
        await meiliTask(
          `/indexes/${INDEX}/documents?primaryKey=id`,
          "POST",
          docs
        );
        collectionCount += docs.length;
        totalIndexed += docs.length;
      }

      console.log(
        `  batch ${Math.floor(offset / BATCH_SIZE) + 1}: ${docs.length} docs`
      );

      if (rows.length < BATCH_SIZE) break;
      offset += BATCH_SIZE;
    }

    console.log(`  total: ${collectionCount} docs`);
  }

  console.log(`\nBackfill complete. ${totalIndexed} documents indexed.`);

  // -------------------------------------------------------------------------
  // Remove stale documents from Meilisearch that no longer exist in Postgres
  // -------------------------------------------------------------------------

  console.log("\nBuilding set of valid document IDs from Postgres...");
  const validIds = new Set<string>();

  for (const collection of collections) {
    let offset = 0;
    while (true) {
      const rows = await sql`
        SELECT uri
        FROM records
        WHERE collection = ${collection}
        ORDER BY uri
        LIMIT ${BATCH_SIZE}
        OFFSET ${offset}
      `;
      if (rows.length === 0) break;
      for (const row of rows) {
        validIds.add(toDocId(row.uri));
      }
      if (rows.length < BATCH_SIZE) break;
      offset += BATCH_SIZE;
    }
  }

  console.log(`  ${validIds.size} valid document IDs.`);

  console.log("Fetching all document IDs from Meilisearch...");
  const staleIds: string[] = [];
  let meiliOffset = 0;
  const meiliBatchSize = 1000;

  while (true) {
    const result = (await meili(
      `/indexes/${INDEX}/documents?fields=id&limit=${meiliBatchSize}&offset=${meiliOffset}`
    )) as { results: { id: string }[]; total: number };

    for (const doc of result.results) {
      if (!validIds.has(doc.id)) {
        staleIds.push(doc.id);
      }
    }

    meiliOffset += result.results.length;
    if (meiliOffset >= result.total || result.results.length === 0) break;
  }

  console.log(`  Found ${staleIds.length} stale documents to remove.`);

  if (staleIds.length > 0) {
    // Delete in batches
    for (let i = 0; i < staleIds.length; i += BATCH_SIZE) {
      const batch = staleIds.slice(i, i + BATCH_SIZE);
      await meiliTask(
        `/indexes/${INDEX}/documents/delete-batch`,
        "POST",
        batch
      );
      console.log(
        `  deleted batch ${Math.floor(i / BATCH_SIZE) + 1}: ${batch.length} docs`
      );
    }
    console.log(`  Removed ${staleIds.length} stale documents.`);
  }

  await sql.end();
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log("Meilisearch backfill starting...");
  console.log(`  Meilisearch: ${MEILI_URL}`);
  console.log(`  Index: ${INDEX}`);
  console.log("");

  await configureIndex();
  await backfill();

  console.log("\nDone.");
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
