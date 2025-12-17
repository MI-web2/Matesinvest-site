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

// Where stock-page email capture posts to
const SIGNUP_ENDPOINT = "/.netlify/functions/subscribe";
const FALLBACK_JOIN_URL = "/join.html";

// Adjust these if your key names differ
const FUND_MASTER_KEY = "asx:universe:fundamentals:latest";
const PRICE_KEY = "asx:universe:eod:latest";

const TEMPLATE_PATH = path.join(__dirname, "stocks", "_template.html");
const OUTPUT_DIR = path.join(__dirname, "stocks");

async function redisGet(key) {
  const res = await fetch(`${UPSTASH_URL}/get/${encodeURIComponent(key)}`, {
    headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
  });

  if (!res.ok) {
    throw new Error(
      `Upstash error for key "${key}": ${res.status} ${res.statusText}`
    );
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

// Safely inject a block before </body>
function injectBeforeBodyClose(html, block, marker) {
  if (!html || typeof html !== "string") return html;
  if (marker && html.includes(marker)) return html; // prevent duplicates
  const idx = html.toLowerCase().lastIndexOf("</body>");
  if (idx === -1) return html + "\n" + block;
  return html.slice(0, idx) + "\n" + block + "\n" + html.slice(idx);
}

// ---------- injections ----------

function trackingSnippet() {
  return `
<!-- MatesInvest: Repeat user tracking -->
<script>
(() => {
  const KEY = "mates_user_id_v1";
  let uid = localStorage.getItem(KEY);
  if (!uid) {
    uid = (crypto?.randomUUID?.() || String(Math.random()).slice(2) + Date.now());
    localStorage.setItem(KEY, uid);
  }

  fetch("/.netlify/functions/track-visit", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      uid,
      path: window.location.pathname,
      ts: Date.now()
    })
  }).catch(() => {});
})();
</script>
`;
}

function emailCaptureBlock({ code, name }) {
  const safeName = String(name || "").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  const safeCode = String(code || "").replace(/</g, "&lt;").replace(/>/g, "&gt;");

  return `
<!-- MatesInvest: Stock page email capture -->
<style>
  .mi-capture {
    max-width: 720px;
    margin: 18px auto 0;
    padding: 14px;
    border: 1px solid rgba(0,0,0,0.08);
    border-radius: 14px;
    background: rgba(255,255,255,0.9);
  }
  .mi-capture h3 { margin: 0 0 6px; font-size: 18px; }
  .mi-capture p { margin: 0 0 10px; color: rgba(0,0,0,0.65); font-size: 14px; }
  .mi-capture .row {
    display: flex;
    gap: 10px;
    flex-wrap: wrap;
    align-items: center;
  }
  .mi-capture input[type="email"]{
    flex: 1 1 240px;
    padding: 12px 12px;
    border-radius: 12px;
    border: 1px solid rgba(0,0,0,0.15);
    font-size: 15px;
    outline: none;
  }
  .mi-capture button{
    padding: 12px 14px;
    border-radius: 12px;
    border: 0;
    cursor: pointer;
    font-size: 15px;
    font-weight: 600;
    background: #111827;
    color: #fff;
  }
  .mi-capture .meta {
    margin-top: 8px;
    font-size: 12px;
    color: rgba(0,0,0,0.55);
  }
  .mi-capture .msg {
    margin-top: 8px;
    font-size: 13px;
  }
</style>

<section class="mi-capture" aria-label="Email signup">
  <h3>Get the free 6:05am ASX brief</h3>
  <p>Daily snapshot + key moves. No spam. Unsubscribe anytime.</p>

  <form id="miEmailForm" class="row">
    <input
      id="miEmail"
      type="email"
      inputmode="email"
      autocomplete="email"
      placeholder="Email address"
      required
    />
    <button id="miEmailBtn" type="submit">Get the email</button>
  </form>

  <div id="miEmailMsg" class="msg" aria-live="polite"></div>
  <div class="meta">You’re viewing: <b>${safeName}</b> (ASX:${safeCode})</div>
</section>

<script>
(() => {
  const form = document.getElementById("miEmailForm");
  const emailEl = document.getElementById("miEmail");
  const btn = document.getElementById("miEmailBtn");
  const msg = document.getElementById("miEmailMsg");
  if (!form || !emailEl || !btn || !msg) return;

  const CODE = ${JSON.stringify(String(code || "").trim())};
  const NAME = ${JSON.stringify(String(name || "").trim())};

  function setMsg(text, ok) {
    msg.textContent = text || "";
    msg.style.color = ok ? "#16a34a" : "#b91c1c";
  }

  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    const email = (emailEl.value || "").trim();
    if (!email) return;

    btn.disabled = true;
    setMsg("Adding you…", true);

    try {
      const res = await fetch(${JSON.stringify(SIGNUP_ENDPOINT)}, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          email,
          source: "stock_page",
          code: CODE,
          name: NAME,
          path: window.location.pathname
        })
      });

      if (!res.ok) throw new Error("bad_status_" + res.status);

      setMsg("You’re in ✅ Check your inbox tomorrow at 6:05am.", true);
      emailEl.value = "";
    } catch (err) {
      setMsg("Couldn’t add you automatically — redirecting…", false);
      const url = ${JSON.stringify(FALLBACK_JOIN_URL)} + "?code=" + encodeURIComponent(CODE);
      setTimeout(() => { window.location.href = url; }, 700);
    } finally {
      btn.disabled = false;
    }
  });
})();
</script>
`;
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

    if (Array.isArray(parsed)) all = all.concat(parsed);
    else if (Array.isArray(parsed.items)) all = all.concat(parsed.items);
    else console.warn(`⚠️ Fundamentals part ${partKey} has unexpected shape`);
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
  const [fundamentals, prices] = await Promise.all([loadFundamentals(), loadPrices()]);
  const sitemapUrls = [];

  const fundItems = fundamentals.items;
  const updatedAt = fundamentals.generatedAt || prices.generatedAt || new Date().toISOString();

  if (!Array.isArray(fundItems) || fundItems.length === 0) {
    console.warn("⚠️ No fundamentals items found, nothing to generate.");
    return;
  }

  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR);

  let count = 0;

  for (const f of fundItems) {
    const code = (f.code || f.ticker || "").trim();
    if (!code) continue;

    const name = f.name || `ASX:${code}`;
    const sector = f.sector || f.GICS_Sector || "Unknown";
    const industry = f.industry || f.GICS_Industry_Group || f.GICS_Industry || "Unknown";

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
        : f.DividendYield != null && isFinite(Number(f.DividendYield) * 100)
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

    // ✅ Email capture: uses the template placeholder
    html = html.replace(/{{EMAIL_CAPTURE}}/g, emailCaptureBlock({ code, name }));

    // ✅ Tracking: inject near </body>
    html = injectBeforeBodyClose(
      html,
      trackingSnippet(),
      "<!-- MatesInvest: Repeat user tracking -->"
    );

    const outPath = path.join(OUTPUT_DIR, `${code}.html`);
    fs.writeFileSync(outPath, html, "utf8");

    sitemapUrls.push(`${SITE_URL}/stocks/${code}.html`);
    count++;
  }

  console.log(`✅ Generated ${count} stock pages in /stocks`);

  const sitemapXml =
    `<?xml version="1.0" encoding="UTF-8"?>\n` +
    `<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n` +
    sitemapUrls
      .map(
        (loc) =>
          `  <url><loc>${loc}</loc><lastmod>${updatedAt.substring(0, 10)}</lastmod></url>`
      )
      .join("\n") +
    `\n</urlset>\n`;

  fs.writeFileSync(path.join(__dirname, "stocks-sitemap.xml"), sitemapXml, "utf8");
}

main().catch((err) => {
  console.error("❌ Error generating stock pages:", err);
  process.exit(1);
});
