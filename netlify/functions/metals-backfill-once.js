#!/usr/bin/env node
// backfill-metals-once.js
// One-off local script to backfill Upstash history:metal:daily for specified canonical symbols.
// Usage:
//   METALS_API_KEY=xxx UPSTASH_REDIS_REST_URL=https://... UPSTASH_REDIS_REST_TOKEN=yyy node backfill-metals-once.js
//
// Edit the 'targets' array or pass symbols via env SYM_LIST="NI,LITH-CAR"

const HISTORY_MONTHS = Number(process.env.HISTORY_MONTHS || 6);
const METALS_API_KEY = process.env.METALS_API_KEY;
const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

if (!METALS_API_KEY || !UPSTASH_URL || !UPSTASH_TOKEN) {
  console.error("Missing required env. Set METALS_API_KEY, UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN.");
  process.exit(1);
}

const fetchWithTimeout = async (url, opts = {}, timeout = 15000) => {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeout);
  try {
    const res = await fetch(url, { ...opts, signal: controller.signal });
    clearTimeout(id);
    return res;
  } catch (e) {
    clearTimeout(id);
    throw e;
  }
};

function toDateISO(d){ return new Date(d).toISOString().slice(0,10); }
function monthsAgoISO(months, from = new Date()){
  const d = new Date(from);
  d.setMonth(d.getMonth() - months);
  return toDateISO(d);
}
function addDaysISO(dateStr, days){
  const d = new Date(dateStr + "T00:00:00Z");
  d.setDate(d.getDate() + days);
  return toDateISO(d);
}

const METAL_UNITS = {
  XAU: "Troy Ounce",
  XAG: "Troy Ounce",
  IRON: "Ton",
  "LITH-CAR": "Ton",
  NI: "Ton",
  URANIUM: "Pound",
};

// Candidate names to try for each canonical symbol — edit if you know better names from Metals-API
const CANDIDATES = {
  NI: ["NI", "NICKEL", "NICKEL_TON", "NICKEL_ORE", "NICKEL.LME"],
  "LITH-CAR": ["LITH-CAR", "LITHIUM", "LITHIUM_CARBONATE", "LITHIUM_CARBONATE_TON", "LITHIUM-CARBONATE"],
};

async function fetchUsdToAud(){
  try{
    let r = await fetchWithTimeout("https://open.er-api.com/v6/latest/USD", {}, 7000);
    const txt = await r.text().catch(()=>"");
    const j = txt ? JSON.parse(txt) : null;
    if (r.ok && j && j.rates && typeof j.rates.AUD === "number") return Number(j.rates.AUD);
  }catch(e){}
  try{
    let r = await fetchWithTimeout("https://api.exchangerate.host/latest?base=USD&symbols=AUD", {}, 7000);
    const txt = await r.text().catch(()=>"");
    const j = txt ? JSON.parse(txt) : null;
    if (r.ok && j && j.rates && typeof j.rates.AUD === "number") return Number(j.rates.AUD);
  }catch(e){}
  return null;
}

async function fetchTimeseries(candidate, canonical, start, end, unitParam){
  const unitQ = unitParam ? `&unit=${encodeURIComponent(unitParam)}` : "";
  const url = `https://metals-api.com/api/timeseries?access_key=${encodeURIComponent(METALS_API_KEY)}&base=USD&symbols=${encodeURIComponent(candidate)}&start_date=${encodeURIComponent(start)}&end_date=${encodeURIComponent(end)}${unitQ}`;
  const res = await fetchWithTimeout(url, {}, 15000);
  const txt = await res.text().catch(()=>"");
  let json = null;
  try{ json = txt ? JSON.parse(txt) : null; }catch(e){ json=null; }
  return { ok: res.ok, status: res.status, bodyText: txt, json };
}

async function redisSet(key, obj){
  const val = encodeURIComponent(JSON.stringify(obj));
  const url = `${UPSTASH_URL}/set/${encodeURIComponent(key)}/${val}`;
  const res = await fetchWithTimeout(url, { method: "POST", headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` } }, 10000);
  const txt = await res.text().catch(()=>"");
  return { ok: res.ok, status: res.status, bodyPreview: txt.slice(0,1000) };
}

(async()=>{
  const symListEnv = (process.env.SYM_LIST || "").trim();
  const targets = symListEnv ? symListEnv.split(",").map(s=>s.trim().toUpperCase()) : ["NI","LITH-CAR"];
  const today = new Date();
  // Metals-API timeseries tends to be valid up to yesterday; use yesterday
  const end = addDaysISO(toDateISO(today), -1);
  const start = monthsAgoISO(HISTORY_MONTHS, today);
  if (start > end) {
    console.error("Computed start > end; adjust HISTORY_MONTHS or dates.");
    process.exit(2);
  }

  const usdToAud = await fetchUsdToAud();
  console.log("USD->AUD:", usdToAud === null ? "(not available, storing USD)" : usdToAud);

  for (const canonical of targets){
    console.log(`\n--- ${canonical} -> trying candidates: ${CANDIDATES[canonical] ? CANDIDATES[canonical].join(",") : canonical}`);
    let success = null;
    const unitParam = METAL_UNITS[canonical] || null;

    const candidates = CANDIDATES[canonical] ? CANDIDATES[canonical] : [canonical];
    for (const cand of candidates){
      try{
        const r = await fetchTimeseries(cand, canonical, start, end, unitParam);
        console.log(`candidate ${cand}: HTTP ${r.status} (ok=${r.ok})`);
        if (!r.ok){
          console.log("  preview:", (r.bodyText || "").slice(0,300));
          continue;
        }
        if (!r.json || r.json.success === false){
          console.log("  api error:", r.json && r.json.error ? r.json.error : "(no json)");
          continue;
        }
        const ratesByDate = r.json.rates || {};
        const dates = Object.keys(ratesByDate).sort();
        const usdKey = `USD${canonical}`;
        const points = [];
        for (const d of dates){
          const dayRates = ratesByDate[d] || {};
          let priceUSD = null;
          if (typeof dayRates[usdKey] === "number" && dayRates[usdKey] > 0){
            priceUSD = dayRates[usdKey];
          } else if (typeof dayRates[cand] === "number" && dayRates[cand] > 0){
            priceUSD = 1 / dayRates[cand];
          }
          if (typeof priceUSD === "number" && Number.isFinite(priceUSD)){
            const value = usdToAud != null ? Number((priceUSD * usdToAud).toFixed(2)) : Number(priceUSD.toFixed(6));
            points.push([d, value]);
          }
        }

        console.log(`  candidate ${cand} returned ${points.length} points (sample: ${points.slice(0,3).map(p=>p.join(":")).join(", ")})`);
        if (points.length > 3){
          success = { candidate: cand, points };
          break;
        } else {
          console.log("  too few points, trying next candidate");
        }
      }catch(err){
        console.log(`  candidate ${cand} fetch error:`, err && err.message ? err.message : String(err));
      }
    }

    if (!success){
      console.error(`❌ No successful candidate for ${canonical}. Check API preview above or extend CANDIDATES list.`);
      continue;
    }

    // persist under canonical key
    const trimmed = success.points.filter(p => p && p[0]);
    const history = {
      symbol: canonical,
      startDate: trimmed.length ? trimmed[0][0] : start,
      endDate: trimmed.length ? trimmed[trimmed.length-1][0] : end,
      lastUpdated: new Date().toISOString(),
      points: trimmed,
      meta: { candidateUsed: success.candidate, storedIn: usdToAud ? "AUD" : "USD", usdToAud: usdToAud || null }
    };

    const key = `history:metal:daily:${canonical}`;
    try{
      const res = await redisSet(key, history);
      console.log(`Wrote ${key}: ok=${res.ok} status=${res.status} preview=${res.bodyPreview && res.bodyPreview.slice(0,200)}`);
    }catch(e){
      console.error("Upstash set failed:", e && e.message ? e.message : e);
    }
  }

  console.log("\nDone.");
})();
