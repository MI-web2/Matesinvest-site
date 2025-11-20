// netlify/functions/newsFeed.js
// Switched from NewsAPI -> MarketAux (https://www.marketaux.com/documentation)
// Exposes the same output shape: { articles: [...] } used by front-end.

exports.handler = async (event) => {
  try {
    const MARKETAUX_TOKEN = process.env.MARKETAUX_API_TOKEN || null;

    // Accept region query param to keep parity with previous behavior
    const qs = event.queryStringParameters || {};
    const region = (qs.region || 'us').toLowerCase(); // default to 'us' for dev parity

    // map region to MarketAux countries param. 'global' => omit countries
    const regionToCountries = {
      au: 'au',
      us: 'us',
      ca: 'ca',
      uk: 'gb'
    };
    const countries = (region === 'global') ? null : (regionToCountries[region] || region);

    // build MarketAux URL
    const base = 'https://api.marketaux.com/v1/news/all';
    const url = new URL(base);

    if (!MARKETAUX_TOKEN) {
      console.warn('MARKETAUX_API_TOKEN not set in environment');
      // Return fallback articles (same format as before)
      const fallback = getFallbackArticles('Missing MARKETAUX_API_TOKEN');
      return { statusCode: 200, body: JSON.stringify({ articles: fallback }) };
    }

    url.searchParams.set('api_token', MARKETAUX_TOKEN);
    // optional filters - keep results English and reasonably sized
    url.searchParams.set('language', 'en');

    // Apply countries if not global
    if (countries) url.searchParams.set('countries', countries);

    // Request page size / limit - MarketAux accepts pagination (page, per_page or limit may vary by plan)
    // We request a reasonable number that maps to the previous pageSize (20)
    // If the API ignores unknown params, it's still fine.
    url.searchParams.set('page', '1');
    url.searchParams.set('per_page', '20');

    // Optionally, you can pass additional filters such as 'industries' or 'symbols' here

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

    // MarketAux typically returns data array in `data` property (see docs)
    const items = Array.isArray(data.data) ? data.data : (Array.isArray(data.articles) ? data.articles : []);

    // Map MarketAux item -> frontend article shape
    const mapped = items.slice(0, 50).map((it, idx) => {
      // MarketAux fields may be named: title, description, published_at, url, source
      const title = it.title || it.headline || '';
      const description = it.description || it.summary || '';
      const urlLink = it.url || it.link || '';
      const source = (typeof it.source === 'string') ? it.source : (it.source && it.source.name) ? it.source.name : (it.publisher || '');
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