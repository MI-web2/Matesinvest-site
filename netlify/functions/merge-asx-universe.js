// merge-asx-universe.js
// Run this locally or as a separate function after you have processed all batches.
// It will fetch each per-part key and merge into a single deduped latest blob:
// writes asx:universe:fundamentals:latest

const fs = require("fs");
const path = require("path");
const fetch = (...args) => global.fetch(...args);

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
if (!UPSTASH_URL || !UPSTASH_TOKEN) {
  console.error("Please set UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN");
  process.exit(1);
}

function readUniverseSync() {
  const candidates = [ path.join(__dirname, "asx-universe.txt"), path.join(process.cwd(), "asx-universe.txt"), path.join(process.cwd(), "netlify", "functions", "asx-universe.txt") ];
  for (const p of candidates) {
    try {
      if (fs.existsSync(p)) {
        const raw = fs.readFileSync(p, "utf8");
        const parts = raw.split(/[\s,]+/).map(s => s.trim()).filter(Boolean);
        return Array.from(new Set(parts.map(c => c.toUpperCase())));
      }
    } catch (e) {}
  }
  throw new Error("asx-universe.txt not found locally (needed to know universeTotal and batch offsets).");
}

// Small helper to GET a key from Upstash REST API
async function redisGet(key) {
  const url = `${UPSTASH_URL}/get/${encodeURIComponent(key)}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` }});
  if (!res.ok) throw new Error(`Upstash GET failed ${res.status}`);
  const j = await res.json();
  return j.result || null; // Upstash returns { result: <value|null> }
}

// Set key
async function redisSet(key, value) {
  const payload = typeof value === "string" ? value : JSON.stringify(value);
  const url = `${UPSTASH_URL}/set/${encodeURIComponent(key)}/${encodeURIComponent(payload)}`;
  const res = await fetch(url, { method: "POST", headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` }});
  if (!res.ok) throw new Error(`Upstash SET failed ${res.status}`);
  return true;
}

(async () => {
  try {
    const universe = readUniverseSync();
    const universeTotal = universe.length;
    const batchSize = Number(process.env.BATCH_SIZE || 200);
    const parts = [];
    for (let offset = 0; offset < universeTotal; offset += batchSize) {
      const key = `asx:universe:fundamentals:part:${offset}`;
      console.log("fetching", key);
      const raw = await redisGet(key);
      if (!raw) {
        console.warn("missing part", key);
        continue;
      }
      let parsed;
      try {
        parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
      } catch (e) {
        parsed = raw;
      }
      if (parsed && Array.isArray(parsed.items)) parts.push(parsed);
    }

    // Merge and dedupe by code (keep first encountered)
    const mergedMap = new Map();
    parts.sort((a,b) => a.batchStart - b.batchStart); // ensure order
    for (const p of parts) {
      for (const it of p.items) {
        const code = it.code;
        if (!mergedMap.has(code)) mergedMap.set(code, it);
      }
    }

    const merged = {
      generatedAt: new Date().toISOString(),
      universeTotal,
      partCount: parts.length,
      count: mergedMap.size,
      items: Array.from(mergedMap.values()),
    };

    await redisSet("asx:universe:fundamentals:latest", merged);
    console.log("merged saved:", merged.count, "items");
  } catch (err) {
    console.error("merge failed:", err && err.message);
    process.exit(2);
  }
})();