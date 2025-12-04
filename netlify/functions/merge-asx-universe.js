// netlify/functions/merge-asx-universe.js
// Robust merge script for per-part snapshots into a single deduped latest blob.
// Supports writing a single large latest; if the write fails due to size (HTTP 431),
// it falls back to writing the merged items into smaller part keys and stores a manifest
// at asx:universe:fundamentals:latest that points to the part keys.
//
// Works as both a Netlify function (exports.handler) and a CLI script.

const fs = require("fs");
const path = require("path");
const fetch = (...args) => global.fetch(...args);

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
if (!UPSTASH_URL || !UPSTASH_TOKEN) {
  const errMsg = "Please set UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN";
  if (require.main === module) {
    console.error(errMsg);
    process.exit(1);
  } else {
    throw new Error(errMsg);
  }
}

function readUniverseSync() {
  const candidates = [
    path.join(__dirname, "asx-universe.txt"),
    path.join(process.cwd(), "asx-universe.txt"),
    path.join(process.cwd(), "netlify", "functions", "asx-universe.txt"),
  ];
  for (const p of candidates) {
    try {
      if (fs.existsSync(p)) {
        const raw = fs.readFileSync(p, "utf8");
        const parts = raw.split(/[\s,]+/).map((s) => s.trim()).filter(Boolean);
        return Array.from(new Set(parts.map((c) => c.toUpperCase())));
      }
    } catch (e) {}
  }
  throw new Error("asx-universe.txt not found locally (needed to know universeTotal and batch offsets).");
}

async function redisGet(key) {
  const url = `${UPSTASH_URL}/get/${encodeURIComponent(key)}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` }});
  if (!res.ok) throw new Error(`Upstash GET failed ${res.status}`);
  const j = await res.json();
  return j.result || null;
}

// Write helper - uses REST set path (value in path). On very large payload this can fail with 431.
async function redisSet(key, value) {
  const payload = typeof value === "string" ? value : JSON.stringify(value);
  const url = `${UPSTASH_URL}/set/${encodeURIComponent(key)}/${encodeURIComponent(payload)}`;
  const res = await fetch(url, { method: "POST", headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` }});
  if (!res.ok) throw new Error(`Upstash SET failed ${res.status}`);
  return true;
}

// Fallback helper to write multiple part keys and a manifest to latest.
async function writeLatestAsParts(baseKey, merged, opts = {}) {
  const chunkSize = opts.chunkSize || 500; // items per fallback part
  const items = merged.items || [];
  const partKeys = [];
  for (let i = 0; i < items.length; i += chunkSize) {
    const chunk = items.slice(i, i + chunkSize);
    const partKey = `${baseKey}:part:${i}`;
    const payload = {
      generatedAt: merged.generatedAt,
      partStart: i,
      length: chunk.length,
      items: chunk,
    };
    await redisSet(partKey, payload);
    partKeys.push(partKey);
  }

  // Manifest stored at the original latest key so screener can detect it
  const manifest = {
    generatedAt: merged.generatedAt,
    universeTotal: merged.universeTotal,
    fallback: true,
    partCount: partKeys.length,
    parts: partKeys,
    count: items.length,
  };
  // use normal redisSet for manifest (should be much smaller)
  await redisSet(baseKey, manifest);
  return manifest;
}

async function runMerge({ requireComplete = true } = {}) {
  const universe = readUniverseSync();
  const universeTotal = universe.length;
  const batchSize = Number(process.env.BATCH_SIZE || 200);

  console.log("merge: universeTotal=", universeTotal, "batchSize=", batchSize, "requireComplete=", requireComplete);

  const expectedParts = [];
  for (let offset = 0; offset < universeTotal; offset += batchSize) expectedParts.push(offset);

  const parts = [];
  const missingParts = [];

  for (const offset of expectedParts) {
    const key = `asx:universe:fundamentals:part:${offset}`;
    console.log("merge: fetching", key);
    const raw = await redisGet(key);
    if (!raw) {
      console.warn("merge: missing part", key);
      missingParts.push(offset);
      continue;
    }
    let parsed;
    try {
      parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
    } catch (e) {
      parsed = raw;
    }

    if (Array.isArray(parsed)) {
      parts.push({ batchStart: offset, batchSize: parsed.length, items: parsed });
      console.log(`merge: part ${offset} is raw array (len=${parsed.length})`);
    } else if (parsed && Array.isArray(parsed.items)) {
      parts.push(parsed);
      console.log(`merge: part ${offset} ok (items=${parsed.items.length})`);
    } else {
      console.warn(`merge: part ${offset} unexpected shape — skipping`);
      missingParts.push(offset);
    }
  }

  if (requireComplete && missingParts.length > 0) {
    throw new Error(`Merge aborted: missing ${missingParts.length} parts: ${missingParts.join(", ")}`);
  }

  if (parts.length === 0) {
    throw new Error("No parts found to merge — aborting (will not overwrite latest).");
  }

  parts.sort((a,b) => (a.batchStart || 0) - (b.batchStart || 0));
  const mergedMap = new Map();
  for (const p of parts) {
    if (!Array.isArray(p.items)) continue;
    for (const it of p.items) {
      if (!it || !it.code) continue;
      const code = String(it.code).toUpperCase();
      if (!mergedMap.has(code)) mergedMap.set(code, it);
    }
  }

  const mergedItems = Array.from(mergedMap.values());
  if (mergedItems.length === 0) throw new Error("Merged items empty — aborting (no write).");

  const merged = {
    generatedAt: new Date().toISOString(),
    universeTotal,
    partCount: parts.length,
    expectedPartCount: expectedParts.length,
    missingParts,
    count: mergedItems.length,
    items: mergedItems,
  };

  const baseLatestKey = "asx:universe:fundamentals:latest";
  const tmpKey = `${baseLatestKey}:tmp:${Date.now()}`;

  console.log("merge: attempting write tmp", tmpKey);

  // Try writing the large blob directly. If Upstash rejects with status 431, fall back.
  try {
    await redisSet(tmpKey, merged);
    await redisSet(baseLatestKey, merged);
    console.log("merge saved as single blob. merged count=", merged.count, "parts merged=", parts.length);
    return { mode: "single", merged };
  } catch (err) {
    console.warn("merge: primary write failed:", err && err.message);
    // detect large payload style error (Upstash SET failed 431) -> fallback
    if (String(err && err.message).includes("431")) {
      console.log("merge: falling back to writing latest as smaller part keys (payload too large)");
      const manifest = await writeLatestAsParts(baseLatestKey, merged, { chunkSize: 500 });
      console.log("merge: written fallback manifest with parts=", manifest.parts.length);
      return { mode: "fallback", merged: manifest };
    }
    // rethrow other errors
    throw err;
  }
}

// Netlify handler
exports.handler = async function (event, context) {
  try {
    const requireCompleteEnv = process.env.MERGE_REQUIRE_COMPLETE;
    const requireComplete = (typeof requireCompleteEnv === "undefined") ? true : String(requireCompleteEnv) === "1";

    const result = await runMerge({ requireComplete });
    return {
      statusCode: 200,
      body: JSON.stringify({ ok: true, mode: result.mode, merged: { generatedAt: result.merged.generatedAt, universeTotal: result.merged.universeTotal, partCount: result.merged.partCount || result.merged.partCount, count: result.merged.count } }),
    };
  } catch (err) {
    console.error("merge handler error:", err && err.message);
    return { statusCode: 500, body: JSON.stringify({ ok: false, error: String(err && err.message) }) };
  }
};

// CLI support
if (require.main === module) {
  (async () => {
    try {
      const requireCompleteEnv = process.env.MERGE_REQUIRE_COMPLETE;
      const requireComplete = (typeof requireCompleteEnv === "undefined") ? true : String(requireCompleteEnv) === "1";
      const result = await runMerge({ requireComplete });
      console.log("CLI merge finished:", { mode: result.mode, generatedAt: result.merged.generatedAt, universeTotal: result.merged.universeTotal, partCount: result.merged.partCount, count: result.merged.count });
      process.exit(0);
    } catch (err) {
      console.error("CLI merge failed:", err && err.message);
      process.exit(2);
    }
  })();
}
