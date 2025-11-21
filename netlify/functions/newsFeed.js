// netlify/functions/newsFeed.js
// MarketAux-backed news feed with optional query passthrough from the UI.
// - Uses MARKETAUX_API_TOKEN (set in Netlify env)
// - Accepts optional frontend query params (whitelisted) and forwards them to MarketAux
// - Keeps same output shape { articles: [...] } for compatibility with the frontend
//
// Usage examples (frontend):
//  - /.netlify/functions/newsFeed?region=au
//  - /.netlify/functions/newsFeed?symbols=AAPL,CSL&page=1&per_page=20
//  - /.netlify/functions/newsFeed?countries=us,ca&language=en&sentiment_gte=0
//
// Security & limits:
//  - Only a safe whitelist of query params is forwarded to MarketAux
//  - per_page is clamped to [1,100] to avoid huge responses
//  - The function returns friendly fallback articles on errors
//
// MarketAux docs: https://www.marketaux.com/documentation

exports.handler = async (event) => {
  try {
    const MARKETAUX_TOKEN = process.env.MARKETAUX_API_TOKEN || null;

    // Query params from the frontend
    const qs = event.queryStringParameters || {};

    // Keep backward-compatible 'region' handling for quick presets (au/us/global)
    // but allow explicit 'countries' param from the UI to override region mapping.
    const region = (qs.region || '').toLowerCase();

    // Map short region -> marketaux countries ISO codes
    const regionToCountries = {
      au: 'au',
      us: 'us',
      ca: 'ca',
      uk: 'gb',
      gb: 'gb',
      nz: 'nz',
      eu: 'eu' // MarketAux may interpret 'eu' differently; test on your plan
    };

    // Whitelist of params we allow the UI to forward to MarketAux
    // (these come from MarketAux docs — only include safe ones you trust)
    const ALLOWED_PARAMS = new Set([
      'symbols',       // comma-separated tickers
      'countries',     // comma-separated ISO country codes
      'industries',
      'entity_types',
      'language',
      'sentiment_gte',
      'sentiment_lte',
      'page',
      'per_page',      // back to original
      'sort_by',       // back to original
      'filter_entities',
      'must_have_entities',
      'group_similar',
      'search',
      'domains'
    ]);




    // Build the MarketAux URL and set the API token
    const base = 'https://api.marketaux.com/v1/news/all';
    const url = new URL(base);

    if (!MARKETAUX_TOKEN) {
      console.warn('MARKETAUX_API_TOKEN not set in environment');
      const fallback = getFallbackArticles('Missing MARKETAUX_API_TOKEN');
      return { statusCode: 200, body: JSON.stringify({ articles: fallback }) };
    }

    url.searchParams.set('api_token', MARKETAUX_TOKEN);
    // Default to newest-first unless the UI overrides it later
    url.searchParams.set('sort_by', 'published_at');




    // Default language to English for consistency
    url.searchParams.set('language', 'en');

    // If the frontend explicitly provided a 'countries' param, respect it.
    // Otherwise, if a region is present and not 'global', map to countries.
    if (qs.countries && typeof qs.countries === 'string' && qs.countries.trim()) {
      // basic sanitization: allow letters, commas, hyphens
      const cleaned = qs.countries.split(',').map(s => s.trim()).filter(Boolean).join(',');
      if (cleaned) url.searchParams.set('countries', cleaned);
    } else if (region && region !== 'global') {
      const mapped = regionToCountries[region] || null;
      if (mapped) url.searchParams.set('countries', mapped);
    }
    // If we're in the AU region and the UI hasn't asked for anything specific,
    // default to "Aussie business" style news:
    //  - country = au
    //  - entity types = equities + indices (ie, company/market news)
    //  - must_have_entities = true (filter out generic macro/politics with no market hook)
    const wantsDefaultAuBusiness =
      region === 'au' &&
      !qs.countries &&
      !qs.symbols &&
      !qs.industries &&
      !qs.entity_types;

    if (wantsDefaultAuBusiness) {
      url.searchParams.set('countries', regionToCountries.au); // 'au'
      url.searchParams.set('entity_types', 'equity,index');
      url.searchParams.set('must_have_entities', 'true');
    }

    // Forward whitelisted params from the UI with validation & sanitization
    for (const key of Object.keys(qs)) {
      if (!ALLOWED_PARAMS.has(key)) continue;
      // we've already handled 'countries' above; skip it here if we set it already
      if (key === 'countries' && url.searchParams.has('countries')) continue;

      const val = qs[key];

      if (key === 'per_page') {
        // clamp per_page to [1, 100]
        let n = parseInt(val, 10);
        if (isNaN(n) || n < 1) n = 1;
        if (n > 100) n = 100;
        url.searchParams.set('per_page', String(n));
        continue;
      }

      if (key === 'page') {
        let p = parseInt(val, 10);
        if (isNaN(p) || p < 1) p = 1;
        if (p > 1000) p = 1; // avoid ridiculous pages
        url.searchParams.set('page', String(p));
        continue;
      }

      if (key === 'symbols' || key === 'industries' || key === 'entity_types') {
        // Sanitise list-like params: allow letters, numbers, dot, hyphen, comma and spaces
        const cleanedList = String(val)
          .split(',')
          .map(s => s.trim())
          .filter(Boolean)
          .map(s => s.replace(/[^A-Za-z0-9\.\-\_]/g, '')) // remove unsafe chars
          .filter(Boolean)
          .join(',');
        if (cleanedList) url.searchParams.set(key, cleanedList);
        continue;
      }

      if (key === 'language') {
        const lang = String(val).replace(/[^a-zA-Z\-]/g, '').toLowerCase();
        if (lang) url.searchParams.set('language', lang);
        continue;
      }

      if (key === 'sentiment_gte' || key === 'sentiment_lte') {
        // numeric between -1 and 1
        let num = parseFloat(val);
        if (!isNaN(num)) {
          if (num < -1) num = -1;
          if (num > 1) num = 1;
          url.searchParams.set(key, String(num));
        }
        continue;
      }

      // fallback: safe encode
      url.searchParams.set(key, String(val));
    }

    // Default page/per_page if not set
    if (!url.searchParams.has('page')) url.searchParams.set('page', '1');
    if (!url.searchParams.has('per_page')) url.searchParams.set('per_page', '20');



    // Perform the fetch with timeout
    const timeoutMs = 9000;
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), timeoutMs);

    let res;
    try {
      res = await fetch(url.toString(), { signal: controller.signal });
    } finally {
      clearTimeout(id);
    }

    if (!res.ok) {
      const txt = await res.text().catch(() => '');
      console.error('MarketAux HTTP error:', res.status, txt);
      const fallback = getFallbackArticles(`MarketAux HTTP ${res.status}`);
      return { statusCode: 200, body: JSON.stringify({ articles: fallback }) };
    }

    const data = await res.json().catch(() => null);
    if (!data) {
      console.warn('MarketAux returned empty body');
      const fallback = getFallbackArticles('MarketAux empty body');
      return { statusCode: 200, body: JSON.stringify({ articles: fallback }) };
    }

    // MarketAux returns array under `data` typically; also accept `articles`
    const items = Array.isArray(data.data) ? data.data : (Array.isArray(data.articles) ? data.articles : []);

    // Map MarketAux item -> frontend article shape
    const mapped = (items || []).slice(0, 50).map((it, idx) => {
      const title = it.title || it.headline || '';
      const description = it.description || it.summary || '';
      const urlLink = it.url || it.link || '';
      // MarketAux may supply source as string or object
      let source = 'MarketAux';
      if (typeof it.source === 'string' && it.source.trim()) source = it.source;
      else if (it.source && typeof it.source.name === 'string') source = it.source.name;
      else if (it.publisher && typeof it.publisher === 'string') source = it.publisher;

      const publishedAt = it.published_at || it.publishedAt || it.time || new Date().toISOString();

      return {
        id: idx,
        title: title,
        description: description,
        url: urlLink || 'https://matesinvest.com',
        source: source || 'MarketAux',
        publishedAt: publishedAt
      };
    });

    if (!mapped.length) {
      const fallback = getFallbackArticles('No MarketAux articles returned');
      return { statusCode: 200, body: JSON.stringify({ articles: fallback }) };
    }

    return {
      statusCode: 200,
      body: JSON.stringify({ articles: mapped })
    };

  } catch (err) {
    console.error('newsFeed function error:', err && (err.stack || err.message || err));
    const fallback = getFallbackArticles('Unexpected server error');
    return {
      statusCode: 200,
      body: JSON.stringify({ articles: fallback })
    };
  }
};

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
