// netlify/functions/merge-asx-universe.js
// Robust merge script for per-part snapshots into a single deduped latest blob.
// This file supports both:
//  - running as a CLI:   node netlify/functions/merge-asx-universe.js
//  - running as a Netlify function: exports.handler
//
// By default it REQUIRES all parts be present before writing latest (safe).
// Set MERGE_REQUIRE_COMPLETE=0 to allow merging from partial parts.
//
// Usage (locally):
//   UPSTASH_REDIS_REST_URL=... UPSTASH_REDIS_REST_TOKEN=... node netlify/functions/merge-asx-universe.js
//
// Usage (Netlify function):
//   Deploy and invoke the function (scheduled or manual).

const fs = require("fs");
const path = require("path");
const fetch = (...args) => global.fetch(...args);

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
if (!UPSTASH_URL || !UPSTASH_TOKEN) {
  // When used as a Netlify function, return an error rather than exiting
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

async function redisSet(key, value) {
  const payload = typeof value === "string" ? value : JSON.stringify(value);
  const url = `${UPSTASH_URL}/set/${encodeURIComponent(key)}/${encodeURIComponent(payload)}`;
  const res = await fetch(url, { method: "POST", headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` }});
  if (!res.ok) throw new Error(`Upstash SET failed ${res.status}`);
  return true;
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

    // Accept either an array of rows, or an object with .items
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

  // Merge and dedupe by code (preserve order by batchStart)
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
  if (mergedItems.length === 0) {
    throw new Error("Merged items empty — aborting (no write).");
  }

  const merged = {
    generatedAt: new Date().toISOString(),
    universeTotal,
    partCount: parts.length,
    expectedPartCount: expectedParts.length,
    missingParts,
    count: mergedItems.length,
    items: mergedItems,
  };

  // Write to a temporary key first, then atomically set latest (Upstash REST has only SET)
  const tmpKey = `asx:universe:fundamentals:latest:tmp:${Date.now()}`;
  console.log("merge: writing tmp", tmpKey);
  await redisSet(tmpKey, merged);
  console.log("merge: moving tmp to latest");
  await redisSet("asx:universe:fundamentals:latest", merged);
  console.log("merge saved. merged count=", merged.count, "parts merged=", parts.length, "missingParts=", missingParts.length);

  return merged;
}

// Netlify handler
exports.handler = async function (event, context) {
  try {
    const requireCompleteEnv = process.env.MERGE_REQUIRE_COMPLETE;
    const requireComplete = (typeof requireCompleteEnv === "undefined") ? true : String(requireCompleteEnv) === "1";

    const merged = await runMerge({ requireComplete });
    return {
      statusCode: 200,
      body: JSON.stringify({ ok: true, merged: { generatedAt: merged.generatedAt, universeTotal: merged.universeTotal, partCount: merged.partCount, count: merged.count, missingParts: merged.missingParts } }),
    };
  } catch (err) {
    console.error("merge handler error:", err && err.message);
    return {
      statusCode: 500,
      body: JSON.stringify({ ok: false, error: String(err && err.message) }),
    };
  }
};

// Allow CLI invocation: node merge-asx-universe.js
if (require.main === module) {
  (async () => {
    try {
      const requireCompleteEnv = process.env.MERGE_REQUIRE_COMPLETE;
      const requireComplete = (typeof requireCompleteEnv === "undefined") ? true : String(requireCompleteEnv) === "1";
      const merged = await runMerge({ requireComplete });
      console.log("CLI merge finished:", { generatedAt: merged.generatedAt, universeTotal: merged.universeTotal, partCount: merged.partCount, count: merged.count, missingParts: merged.missingParts });
      process.exit(0);
    } catch (err) {
      console.error("CLI merge failed:", err && err.message);
      process.exit(2);
    }
  })();
}
