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

  // Ranking rules — boost exact name matches
  await meiliTask(`/indexes/${INDEX}/settings/ranking-rules`, "PUT", [
    "words",
    "typo",
    "proximity",
    "attribute",
    "sort",
    "exactness",
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
        ...(slug ? { slug } : {}),
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
        ...(slug ? { slug } : {}),
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

  // Load all slug records into a map: ref → slug
  console.log("Loading slugs...");
  const slugRows = await sql`
    SELECT record->>'ref' AS ref, record->>'slug' AS slug
    FROM records
    WHERE collection = 'games.gamesgamesgamesgames.slug'
      AND record->>'ref' IS NOT NULL
      AND record->>'slug' IS NOT NULL
  `;
  const slugsByRef = new Map<string, string>();
  for (const row of slugRows) {
    slugsByRef.set(row.ref, row.slug);
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
