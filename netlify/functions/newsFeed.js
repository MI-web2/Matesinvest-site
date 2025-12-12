// netlify/functions/newsFeed.js
// Combined MarketAux + EODHD-backed news feed.
// - Uses MARKETAUX_API_TOKEN for global news
// - Uses EODHD_API_TOKEN (or EODHD_API_KEY) for ASX news
// - Frontend still sees { articles: [...] } in the same shape
//
// Frontend examples:
//  - /.netlify/functions/newsFeed?region=au
//  - /.netlify/functions/newsFeed?symbols=AAPL,CSL&page=1&per_page=20
//  - /.netlify/functions/newsFeed?countries=us,ca&language=en&sentiment_gte=0
//  - /.netlify/functions/newsFeed?q=iron%20ore  (NEW)

exports.handler = async (event) => {
  try {
    const MARKETAUX_TOKEN = process.env.MARKETAUX_API_TOKEN || null;
    const EODHD_TOKEN =
      process.env.EODHD_API_TOKEN || process.env.EODHD_API_KEY || null;

    const qs = event.queryStringParameters || {};
    const region = (qs.region || "au").toLowerCase();

    // NEW: unified query param for full-universe search
    // Accept both q= and search= (so you can call either)
    const qRaw =
      (typeof qs.q === "string" && qs.q.trim()) ||
      (typeof qs.search === "string" && qs.search.trim()) ||
      "";
    const q = qRaw.trim();

    const regionToCountries = {
      au: "au",
      us: "us",
      ca: "ca",
      uk: "gb",
      gb: "gb",
      nz: "nz",
      eu: "eu",
    };

    const ALLOWED_PARAMS = new Set([
      "symbols",
      "countries",
      "industries",
      "entity_types",
      "language",
      "sentiment_gte",
      "sentiment_lte",
      "page",
      "per_page",
      "sort_by",
      "filter_entities",
      "must_have_entities",
      "group_similar",
      "search", // MarketAux uses this
      "domains",
    ]);

    // ---------- Build Marketaux URL (existing logic) ----------
    let marketauxUrl = null;

    if (MARKETAUX_TOKEN) {
      const base = "https://api.marketaux.com/v1/news/all";
      const url = new URL(base);

      url.searchParams.set("api_token", MARKETAUX_TOKEN);
      url.searchParams.set("sort_by", "published_at");
      url.searchParams.set("language", "en");

      if (
        qs.countries &&
        typeof qs.countries === "string" &&
        qs.countries.trim()
      ) {
        const cleaned = qs.countries
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
          .join(",");
        if (cleaned) url.searchParams.set("countries", cleaned);
      } else if (region && region !== "global") {
        const mapped = regionToCountries[region] || null;
        if (mapped) url.searchParams.set("countries", mapped);
      }

      const wantsDefaultAuBusiness =
        region === "au" &&
        !qs.countries &&
        !qs.symbols &&
        !qs.industries &&
        !qs.entity_types &&
        !q;

      if (wantsDefaultAuBusiness) {
        url.searchParams.set("countries", regionToCountries.au);
        url.searchParams.set("entity_types", "equity,index");
        url.searchParams.set("must_have_entities", "true");
        url.searchParams.set("filter_entities", "true");
        url.searchParams.set("group_similar", "true");
      }

      // NEW: if q is provided, push it into MarketAux search
      if (q) {
        url.searchParams.set("search", q);
        // When searching, it's usually better to not group similar too aggressively
        // (but keep whatever the user passed)
        if (!qs.group_similar) url.searchParams.set("group_similar", "false");
      }

      for (const key of Object.keys(qs)) {
        if (!ALLOWED_PARAMS.has(key)) continue;
        // Prevent overriding our computed countries mapping
        if (key === "countries" && url.searchParams.has("countries")) continue;
        // Prevent overriding our unified q mapping (q/search are handled above)
        if (key === "search" && q) continue;

        const val = qs[key];

        if (key === "per_page") {
          let n = parseInt(val, 10);
          if (isNaN(n) || n < 1) n = 1;
          if (n > 100) n = 100;
          url.searchParams.set("per_page", String(n));
          continue;
        }

        if (key === "page") {
          let p = parseInt(val, 10);
          if (isNaN(p) || p < 1) p = 1;
          if (p > 1000) p = 1;
          url.searchParams.set("page", String(p));
          continue;
        }

        if (
          key === "symbols" ||
          key === "industries" ||
          key === "entity_types"
        ) {
          const cleanedList = String(val)
            .split(",")
            .map((s) => s.trim())
            .filter(Boolean)
            .map((s) => s.replace(/[^A-Za-z0-9.\-_]/g, ""))
            .filter(Boolean)
            .join(",");
          if (cleanedList) url.searchParams.set(key, cleanedList);
          continue;
        }

        if (key === "language") {
          const lang = String(val)
            .replace(/[^a-zA-Z\-]/g, "")
            .toLowerCase();
          if (lang) url.searchParams.set("language", lang);
          continue;
        }

        if (key === "sentiment_gte" || key === "sentiment_lte") {
          let num = parseFloat(val);
          if (!isNaN(num)) {
            if (num < -1) num = -1;
            if (num > 1) num = 1;
            url.searchParams.set(key, String(num));
          }
          continue;
        }

        url.searchParams.set(key, String(val));
      }

      if (!url.searchParams.has("page")) url.searchParams.set("page", "1");
      if (!url.searchParams.has("per_page"))
        url.searchParams.set("per_page", q ? "50" : "20"); // search can return more
      marketauxUrl = url;
    }

    // ---------- Fetch Marketaux + EODHD in parallel ----------
    const [marketauxArticles, eodhdArticles] = await Promise.all([
      fetchMarketauxArticles(marketauxUrl),
      fetchEodhdArticles(EODHD_TOKEN, qs, region, q),
    ]);

    let combined = [...marketauxArticles, ...eodhdArticles];

    // NEW: If EODHD doesn't support query well, ensure we still filter server-side when q exists
    if (q) {
      const ql = q.toLowerCase();
      combined = combined.filter((a) => {
        const title = (a.title || "").toLowerCase();
        const desc = (a.description || "").toLowerCase();
        const src = (a.source || "").toLowerCase();
        const url = (a.url || "").toLowerCase();
        return (
          title.includes(ql) ||
          desc.includes(ql) ||
          src.includes(ql) ||
          url.includes(ql)
        );
      });
    }

    if (!combined.length) {
      const reasonPieces = [];
      if (!MARKETAUX_TOKEN)
        reasonPieces.push("Missing MARKETAUX_API_TOKEN or no Marketaux data");
      if (!EODHD_TOKEN)
        reasonPieces.push("Missing EODHD_API_TOKEN or no EODHD data");
      const reason =
        reasonPieces.join(" · ") || "No MarketAux/EODHD articles returned";
      const fallback = getFallbackArticles(reason);
      return {
        statusCode: 200,
        body: JSON.stringify({ articles: fallback }),
      };
    }

    // Sort newest -> oldest
    combined.sort((a, b) => {
      const da = new Date(a.publishedAt || a.published || 0);
      const db = new Date(b.publishedAt || b.published || 0);
      return db - da;
    });

    // Limit & give stable IDs
    // Default: 50; when searching: 50 (already plenty for UI)
    const cap = 50;
    const limited = combined.slice(0, cap).map((art, idx) => ({
      ...art,
      id: idx,
    }));

    return {
      statusCode: 200,
      body: JSON.stringify({ articles: limited }),
    };
  } catch (err) {
    console.error(
      "newsFeed function error:",
      err && (err.stack || err.message || err)
    );
    const fallback = getFallbackArticles("Unexpected server error");
    return {
      statusCode: 200,
      body: JSON.stringify({ articles: fallback }),
    };
  }
};

/* ------------ Provider helpers ------------ */

// Marketaux: keep same shape as your old code
async function fetchMarketauxArticles(url) {
  if (!url) {
    console.warn("Marketaux not configured – skipping");
    return [];
  }

  const timeoutMs = 9000;
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(url.toString(), { signal: controller.signal });
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      console.error("MarketAux HTTP error:", res.status, txt);
      return [];
    }

    const data = await res.json().catch(() => null);
    if (!data) {
      console.warn("MarketAux returned empty body");
      return [];
    }

    const items = Array.isArray(data.data)
      ? data.data
      : Array.isArray(data.articles)
      ? data.articles
      : [];

    const mapped = (items || []).map((it) => {
      const title = it.title || it.headline || "";
      const description = it.description || it.summary || "";
      const urlLink = it.url || it.link || "";
      let source = "MarketAux";
      if (typeof it.source === "string" && it.source.trim())
        source = it.source;
      else if (it.source && typeof it.source.name === "string")
        source = it.source.name;
      else if (it.publisher && typeof it.publisher === "string")
        source = it.publisher;

      const publishedAt =
        it.published_at || it.publishedAt || it.time || new Date().toISOString();

      return {
        id: 0, // overwritten later
        title: title,
        description: description,
        url: urlLink || "https://matesinvest.com",
        source: source || "MarketAux",
        publishedAt: publishedAt,
      };
    });

    return mapped;
  } catch (e) {
    console.error("MarketAux fetch failed:", e);
    return [];
  } finally {
    clearTimeout(id);
  }
}

// EODHD ASX / Australia-focused news
async function fetchEodhdArticles(EODHD_TOKEN, qs, region, q) {
  try {
    if (!EODHD_TOKEN) {
      console.warn("EODHD not configured – skipping");
      return [];
    }

    // Only use EODHD when we’re in the AU region for now
    if (region !== "au") return [];

    const base = "https://eodhd.com/api/news";
    const url = new URL(base);

    url.searchParams.set("api_token", EODHD_TOKEN);
    url.searchParams.set("fmt", "json");

    // --- HOW WE FOCUS IT ON AUSTRALIA ---
    let mappedTickers = null;
    if (qs.symbols) {
      const symbols = String(qs.symbols)
        .split(",")
        .map((s) => s.trim().toUpperCase())
        .filter(Boolean);

      if (symbols.length) {
        mappedTickers = symbols
          .map((sym) => {
            if (sym.includes(".")) return sym;
            return `${sym}.AU`;
          })
          .join(",");
      }
    }

    if (mappedTickers) {
      url.searchParams.set("s", mappedTickers);
    } else {
      url.searchParams.set("t", "australia");
    }

    // NEW: If a query is supplied, ask EODHD to search it as well (if supported).
    // If EODHD ignores it, we still do server-side filtering in the handler.
    if (q) {
      url.searchParams.set("q", q);
    }

    // --- limit & basic filters ---
    let limit = 20;
    if (qs.per_page) {
      let n = parseInt(qs.per_page, 10);
      if (!isNaN(n) && n > 0 && n <= 100) limit = n;
    } else if (q) {
      limit = 50; // when searching, return more candidates
    }
    url.searchParams.set("limit", String(limit));

    if (qs.from) url.searchParams.set("from", String(qs.from));
    if (qs.to) url.searchParams.set("to", String(qs.to));

    const res = await fetch(url.toString());
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      console.error("EODHD HTTP error:", res.status, txt);
      return [];
    }

    const data = await res.json().catch(() => null);
    if (!Array.isArray(data)) {
      console.warn("EODHD news returned non-array payload");
      return [];
    }

    const mapped = data.map((it) => {
      const title = it.title || "";
      const description = it.content || "";
      const urlLink = it.link || it.url || "https://matesinvest.com";
      const source = it.source || "EODHD (AU news)";
      const publishedAt = it.date || it.published || new Date().toISOString();

      return {
        id: 0, // overwritten later
        title,
        description,
        url: urlLink,
        source,
        publishedAt,
      };
    });

    return mapped;
  } catch (e) {
    console.error("EODHD fetch failed:", e);
    return [];
  }
}

/* ------------ Fallback demo articles ------------ */

function getFallbackArticles(reason) {
  const now = new Date().toISOString();
  return [
    {
      id: 0,
      title: "Demo: MatesSummaries prototype is live",
      description: `Fallback headlines being used (${reason}). This item shows how summaries will look for real articles.`,
      url: "https://matesinvest.com",
      source: "MatesInvest",
      publishedAt: now,
    },
    {
      id: 1,
      title: "Demo: ASX announcements + AI summaries coming soon",
      description:
        "Live ASX company announcements will appear here with MatesInvest summaries underneath.",
      url: "https://matesinvest.com",
      source: "MatesInvest",
      publishedAt: now,
    },
    {
      id: 2,
      title: "Demo: Personal daily briefings for your holdings",
      description:
        "Users will get a personalised, plain-English morning summary based on their real portfolio.",
      url: "https://matesinvest.com",
      source: "MatesInvest",
      publishedAt: now,
    },
  ];
}
