// generate-stock-pages.js
// Builds static /stocks/{CODE}.html pages from Upstash fundamentals + price snapshot.
//
// REQUIREMENTS (Netlify env):
//   UPSTASH_REDIS_REST_URL
//   UPSTASH_REDIS_REST_TOKEN
//
// KEYS (adjust if needed):
//   FUND_MASTER_KEY  -> master pointer for fundamentals (with .parts[])
//   PRICE_KEY        -> latest EOD/screener snapshot with price + pct change

const fs = require("fs");
const path = require("path");
const fetch = (...args) => global.fetch(...args);

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
const SITE_URL = "https://matesinvest.com"; // change if you use a different domain


// 🔧 Adjust these if your key names differ
const FUND_MASTER_KEY = "asx:universe:fundamentals:latest";
// e.g. could also be "asx:universe:eod:latest" or "equity-screener:latest" if that's what you use
const PRICE_KEY = "asx:universe:eod:latest";

const TEMPLATE_PATH = path.join(__dirname, "stocks", "_template.html");
const OUTPUT_DIR = path.join(__dirname, "stocks");

async function redisGet(key) {
  const res = await fetch(`${UPSTASH_URL}/get/${encodeURIComponent(key)}`, {
    headers: {
      Authorization: `Bearer ${UPSTASH_TOKEN}`,
    },
  });

  if (!res.ok) {
    throw new Error(`Upstash error for key "${key}": ${res.status} ${res.statusText}`);
  }

  const data = await res.json();
  return data.result;
}

// ---------- small helpers ----------

function formatMoney(n) {
  if (n == null) return "–";
  const num = Number(n);
  if (!isFinite(num)) return "–";
  if (num >= 1e9) return `$${(num / 1e9).toFixed(1)}b`;
  if (num >= 1e6) return `$${(num / 1e6).toFixed(1)}m`;
  if (num >= 1e3) return `$${(num / 1e3).toFixed(1)}k`;
  return `$${num.toFixed(2)}`;
}

function formatPercent(n) {
  if (n == null) return "0.0";
  const num = Number(n);
  if (!isFinite(num)) return "0.0";
  return num.toFixed(1);
}

function pctColor(pct) {
  const n = Number(pct);
  if (!isFinite(n) || n === 0) return "var(--muted)";
  if (n > 0) return "#16a34a"; // up green
  return "#dc2626"; // down red
}

// ---------- main generator ----------

async function loadFundamentals() {
  const masterRaw = await redisGet(FUND_MASTER_KEY);
  if (!masterRaw) throw new Error(`No fundamentals master found at ${FUND_MASTER_KEY}`);

  let master;
  try {
    master = JSON.parse(masterRaw);
  } catch (e) {
    throw new Error(`Failed to parse fundamentals master JSON: ${e.message}`);
  }

  const parts = master.parts || [];
  const generatedAt = master.generatedAt || new Date().toISOString();

  if (!Array.isArray(parts) || parts.length === 0) {
    throw new Error("Fundamentals master has no parts[]");
  }

  let all = [];

  for (const partKey of parts) {
    const partRaw = await redisGet(partKey);
    if (!partRaw) continue;

    let parsed;
    try {
      parsed = JSON.parse(partRaw);
    } catch (e) {
      console.warn(`⚠️ Failed to parse fundamentals part ${partKey}:`, e.message);
      continue;
    }

    // handle possible shapes:
    // {items:[...]}  OR  [...]
    if (Array.isArray(parsed)) {
      all = all.concat(parsed);
    } else if (Array.isArray(parsed.items)) {
      all = all.concat(parsed.items);
    } else {
      console.warn(`⚠️ Fundamentals part ${partKey} has unexpected shape`);
    }
  }

  return { items: all, generatedAt };
}

async function loadPrices() {
  const raw = await redisGet(PRICE_KEY);
  if (!raw) {
    console.warn(`⚠️ No price snapshot found at ${PRICE_KEY}`);
    return { byCode: {}, generatedAt: null };
  }

  let payload;
  try {
    payload = JSON.parse(raw);
  } catch (e) {
    throw new Error(`Failed to parse price snapshot JSON: ${e.message}`);
  }

  const items = payload.items || payload || [];
  const generatedAt = payload.generatedAt || new Date().toISOString();

  const byCode = {};

  if (!Array.isArray(items)) {
    console.warn("⚠️ Price payload items is not an array");
    return { byCode, generatedAt };
  }

  for (const item of items) {
    const code = (item.code || item.ticker || "").trim();
    if (!code) continue;

    byCode[code] = {
      price: item.price ?? item.close ?? item.last,
      changePct: item.changePct ?? item.pctChange ?? item.change_percent,
      marketCap: item.marketCap ?? item.market_cap,
      volume: item.volume,
    };
  }

  return { byCode, generatedAt };
}

async function main() {
  console.log("🔧 Generating static stock pages…");

  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    throw new Error("Missing UPSTASH_REDIS_REST_URL or UPSTASH_REDIS_REST_TOKEN");
  }

  const template = fs.readFileSync(TEMPLATE_PATH, "utf8");

  const [fundamentals, prices] = await Promise.all([
    loadFundamentals(),
    loadPrices(),
  ]);
  const sitemapUrls = [];

  const fundItems = fundamentals.items;
  const updatedAt =
    fundamentals.generatedAt ||
    prices.generatedAt ||
    new Date().toISOString();

  if (!Array.isArray(fundItems) || fundItems.length === 0) {
    console.warn("⚠️ No fundamentals items found, nothing to generate.");
    return;
  }

  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR);
  }

  let count = 0;

  for (const f of fundItems) {
    const code = (f.code || f.ticker || "").trim();
    if (!code) continue;

    const name = f.name || `ASX:${code}`;
    const sector = f.sector || f.GICS_Sector || "Unknown";
    const industry =
      f.industry || f.GICS_Industry_Group || f.GICS_Industry || "Unknown";

    const priceInfo = prices.byCode[code] || {};

    const price =
      priceInfo.price != null && isFinite(Number(priceInfo.price))
        ? Number(priceInfo.price).toFixed(2)
        : "–";

    const pctChange = formatPercent(priceInfo.changePct);
    const marketCap = formatMoney(
      f.marketCap || f.MarketCapitalization || priceInfo.marketCap
    );

    const pe =
      f.pe != null && isFinite(Number(f.pe))
        ? Number(f.pe).toFixed(1)
        : f.peTTM != null && isFinite(Number(f.peTTM))
        ? Number(f.peTTM).toFixed(1)
        : "–";

    const divYield =
      f.dividendYield != null && isFinite(Number(f.dividendYield) * 100)
        ? (Number(f.dividendYield) * 100).toFixed(1) + "%"
        : f.DividendYield != null &&
          isFinite(Number(f.DividendYield) * 100)
        ? (Number(f.DividendYield) * 100).toFixed(1) + "%"
        : "–";

    const summary =
      f.description ||
      f.longDescription ||
      `${name} (ASX:${code}) sits in the ${sector} sector (${industry}). This page is auto-generated from the MatesInvest ASX Explorer and refreshed daily. Not financial advice.`;

    let html = template
      .replace(/{{CODE}}/g, code)
      .replace(/{{NAME}}/g, name)
      .replace(/{{SECTOR}}/g, sector)
      .replace(/{{INDUSTRY}}/g, industry)
      .replace(/{{PRICE}}/g, price)
      .replace(/{{PCTCHANGE}}/g, pctChange)
      .replace(/{{PCTCOLOR}}/g, pctColor(pctChange))
      .replace(/{{MARKETCAP}}/g, marketCap)
      .replace(/{{PE}}/g, pe)
      .replace(/{{YIELD}}/g, divYield)
      .replace(/{{SUMMARY}}/g, summary)
      .replace(/{{UPDATED_AT}}/g, updatedAt.substring(0, 10));

    const outPath = path.join(OUTPUT_DIR, `${code}.html`);
    fs.writeFileSync(outPath, html, "utf8");
        sitemapUrls.push(`${SITE_URL}/stocks/${code}.html`);

    count++;
  }

  console.log(`✅ Generated ${count} stock pages in /stocks`);
    // Write a dedicated sitemap for stock pages
  const sitemapXml =
    `<?xml version="1.0" encoding="UTF-8"?>\n` +
    `<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n` +
    sitemapUrls
      .map(
        (loc) =>
          `  <url><loc>${loc}</loc><lastmod>${updatedAt.substring(
            0,
            10
          )}</lastmod></url>`
      )
      .join("\n") +
    `\n</urlset>\n`;

  fs.writeFileSync(path.join(__dirname, "stocks-sitemap.xml"), sitemapXml, "utf8");

}

main().catch((err) => {
  console.error("❌ Error generating stock pages:", err);
  process.exit(1);
});
