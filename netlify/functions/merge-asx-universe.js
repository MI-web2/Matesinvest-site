// netlify/functions/merge-asx-universe.js
// Robust merge script for per-part snapshots into a single deduped latest blob.
// NOW: discovers all part keys via SCAN so BATCH_SIZE changes don't break merging.
//
// Keys read:
//   asx:universe:fundamentals:part:*
//
// Keys written:
//   asx:universe:fundamentals:latest   (single blob OR manifest fallback)
//
// Env required:
//   UPSTASH_REDIS_REST_URL
//   UPSTASH_REDIS_REST_TOKEN
//
// Optional env:
//   MERGE_REQUIRE_COMPLETE ("1" default true; set "0" to not fail merge if missing universe codes)
//   MERGE_SCAN_COUNT (default 1000)

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
  throw new Error(
    "asx-universe.txt not found locally (needed to validate completeness)."
  );
}

async function redisGet(key) {
  const url = `${UPSTASH_URL}/get/${encodeURIComponent(key)}`;
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
  });
  if (!res.ok) throw new Error(`Upstash GET failed ${res.status}`);
  const j = await res.json();
  return j.result || null;
}

// Write helper - uses REST set path (value in path). On very large payload this can fail with 431.
async function redisSet(key, value) {
  const payload = typeof value === "string" ? value : JSON.stringify(value);
  const url = `${UPSTASH_URL}/set/${encodeURIComponent(key)}/${encodeURIComponent(
    payload
  )}`;
  const res = await fetch(url, {
    method: "POST",
    headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
  });
  if (!res.ok) throw new Error(`Upstash SET failed ${res.status}`);
  return true;
}

// SCAN helper (batch-size independent discovery of part keys).
async function redisScanAllKeys(matchPattern, count = 1000) {
  // Upstash REST typically supports /scan/<cursor>?match=...&count=...
  // We'll try a conservative approach.
  let cursor = "0";
  const keys = [];

  while (true) {
    const url = `${UPSTASH_URL}/scan/${cursor}?match=${encodeURIComponent(
      matchPattern
    )}&count=${encodeURIComponent(String(count))}`;

    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
    });

    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      throw new Error(`Upstash SCAN failed ${res.status}: ${txt.slice(0, 200)}`);
    }

    const j = await res.json();
    // Upstash commonly returns { result: [nextCursor, [keys...]] }
    const result = j && j.result;
    if (!result || !Array.isArray(result) || result.length < 2) {
      throw new Error("Upstash SCAN returned unexpected shape");
    }

    const nextCursor = String(result[0]);
    const batchKeys = Array.isArray(result[1]) ? result[1] : [];
    for (const k of batchKeys) keys.push(k);

    if (nextCursor === "0") break;
    cursor = nextCursor;
  }

  return keys;
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

  const manifest = {
    generatedAt: merged.generatedAt,
    universeTotal: merged.universeTotal,
    fallback: true,
    partCount: partKeys.length,
    parts: partKeys,
    count: items.length,
  };

  await redisSet(baseKey, manifest);
  return manifest;
}

function extractBatchStartFromKey(key) {
  // key: asx:universe:fundamentals:part:<offset>
  const m = String(key).match(/:part:(\d+)$/);
  return m ? Number(m[1]) : null;
}

async function runMerge({ requireComplete = true } = {}) {
  const universe = readUniverseSync();
  const universeTotal = universe.length;
  const universeSet = new Set(universe);

  const scanCount = Number(process.env.MERGE_SCAN_COUNT || 1000);
  const partPattern = "asx:universe:fundamentals:part:*";

  console.log(
    "merge: universeTotal=",
    universeTotal,
    "requireComplete=",
    requireComplete,
    "scanCount=",
    scanCount
  );

  // Discover part keys (batch-size independent)
  const allKeys = await redisScanAllKeys(partPattern, scanCount);
  const partKeys = allKeys
    .filter((k) => /:part:\d+$/.test(String(k)))
    .sort((a, b) => (extractBatchStartFromKey(a) || 0) - (extractBatchStartFromKey(b) || 0));

  if (partKeys.length === 0) {
    throw new Error("No part keys found to merge — aborting (will not overwrite latest).");
  }

  console.log("merge: discovered partKeys=", partKeys.length);

  const parts = [];
  const badParts = [];

  for (const key of partKeys) {
    console.log("merge: fetching", key);
    const raw = await redisGet(key);
    if (!raw) {
      console.warn("merge: missing/empty part", key);
      badParts.push({ key, reason: "missing" });
      continue;
    }

    let parsed;
    try {
      parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
    } catch (e) {
      parsed = raw;
    }

    const batchStart = extractBatchStartFromKey(key);

    if (Array.isArray(parsed)) {
      parts.push({ batchStart, items: parsed });
      console.log(`merge: part ${key} raw array (len=${parsed.length})`);
    } else if (parsed && Array.isArray(parsed.items)) {
      parts.push({ batchStart: parsed.batchStart ?? batchStart, items: parsed.items });
      console.log(`merge: part ${key} ok (items=${parsed.items.length})`);
    } else {
      console.warn(`merge: part ${key} unexpected shape — skipping`);
      badParts.push({ key, reason: "unexpected-shape" });
    }
  }

  if (parts.length === 0) {
    throw new Error("No valid parts found to merge — aborting (will not overwrite latest).");
  }

  parts.sort((a, b) => (a.batchStart || 0) - (b.batchStart || 0));

  // Dedup by code
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

  // Completeness check against universe list (optional strictness)
  const mergedCodes = new Set(Array.from(mergedMap.keys()));
  const missingCodes = [];
  for (const c of universeSet) {
    if (!mergedCodes.has(c)) missingCodes.push(c);
  }

  if (requireComplete && missingCodes.length > 0) {
    // Keep error small (don’t dump thousands of codes)
    throw new Error(
      `Merge aborted: missing ${missingCodes.length} codes (example: ${missingCodes.slice(0, 20).join(", ")})`
    );
  }

  const merged = {
    generatedAt: new Date().toISOString(),
    universeTotal,
    partKeyCount: partKeys.length,
    partCount: parts.length,
    badParts,
    count: mergedItems.length,
    missingCodeCount: missingCodes.length,
    items: mergedItems,
  };

  const baseLatestKey = "asx:universe:fundamentals:latest";
  const tmpKey = `${baseLatestKey}:tmp:${Date.now()}`;

  console.log("merge: attempting write tmp", tmpKey);

  try {
    await redisSet(tmpKey, merged);
    await redisSet(baseLatestKey, merged);
    console.log(
      "merge saved as single blob. merged count=",
      merged.count,
      "parts merged=",
      parts.length
    );
    return { mode: "single", merged };
  } catch (err) {
    console.warn("merge: primary write failed:", err && err.message);
    if (String(err && err.message).includes("431")) {
      console.log("merge: falling back to writing latest as smaller part keys (payload too large)");
      const manifest = await writeLatestAsParts(baseLatestKey, merged, { chunkSize: 500 });
      console.log("merge: written fallback manifest with parts=", manifest.parts.length);
      return { mode: "fallback", merged: manifest };
    }
    throw err;
  }
}

// Netlify handler
exports.handler = async function () {
  try {
    const requireCompleteEnv = process.env.MERGE_REQUIRE_COMPLETE;
    const requireComplete =
      typeof requireCompleteEnv === "undefined" ? true : String(requireCompleteEnv) === "1";

    const result = await runMerge({ requireComplete });
    return {
      statusCode: 200,
      body: JSON.stringify({
        ok: true,
        mode: result.mode,
        merged: {
          generatedAt: result.merged.generatedAt,
          universeTotal: result.merged.universeTotal,
          partCount: result.merged.partCount || result.merged.partCount,
          count: result.merged.count,
          missingCodeCount: result.merged.missingCodeCount || 0,
        },
      }),
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
      const requireComplete =
        typeof requireCompleteEnv === "undefined" ? true : String(requireCompleteEnv) === "1";

      const result = await runMerge({ requireComplete });
      console.log("CLI merge finished:", {
        mode: result.mode,
        generatedAt: result.merged.generatedAt,
        universeTotal: result.merged.universeTotal,
        partCount: result.merged.partCount,
        count: result.merged.count,
        missingCodeCount: result.merged.missingCodeCount,
      });
      process.exit(0);
    } catch (err) {
      console.error("CLI merge failed:", err && err.message);
      process.exit(2);
    }
  })();
}
