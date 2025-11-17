// netlify/functions/morning-brief.js
// Serverless function: fetches market headlines (NewsAPI) and asks OpenAI to compose a morning brief.
// Requires env vars: NEWSAPI_KEY and OPENAI_API_KEY
export async function handler(event) {
  try {
    const NEWSAPI_KEY = process.env.NEWSAPI_KEY;
    const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

    if (!NEWSAPI_KEY || !OPENAI_API_KEY) {
      return { statusCode: 500, body: JSON.stringify({ error: 'Missing API keys (NEWSAPI_KEY / OPENAI_API_KEY).' }) };
    }

    const params = event.queryStringParameters || {};
    const region = (params.region || 'au').toLowerCase();

    // Build a NewsAPI query tailored to the region
    // For AU we give preference to ASX/Wall St linkage; for US/global adjust queries.
    let q = 'market OR stocks OR ASX OR S&P OR NASDAQ OR futures OR inflation OR rates';
    if (region === 'au') {
      q = 'ASX OR Australia OR ASX200 OR "Australian share" OR market OR stocks';
    } else if (region === 'us') {
      q = 'Wall Street OR S&P OR NASDAQ OR US economy OR stocks OR futures';
    } else {
      q = 'markets OR stocks OR global markets OR futures';
    }

    const newsUrl = `https://newsapi.org/v2/everything?q=${encodeURIComponent(q)}&language=en&pageSize=12&sortBy=publishedAt`;
    const newsRes = await fetch(newsUrl, {
      headers: { 'X-Api-Key': NEWSAPI_KEY }
    });

    if (!newsRes.ok) {
      const txt = await newsRes.text();
      return { statusCode: newsRes.status, body: JSON.stringify({ error: 'NewsAPI error', details: txt }) };
    }

    const newsData = await newsRes.json();
    const articles = (newsData.articles || []).map(a => ({
      title: a.title,
      source: a.source && a.source.name,
      url: a.url,
      publishedAt: a.publishedAt,
      description: a.description || ''
    }));

    // Build prompt for OpenAI - ask for structured JSON response to simplify client rendering
    const prompt = `
You are a market briefing assistant writing "Morning Briefing" for Australian retail investors.
Produce a concise briefing using the input headlines below. Output JSON only with these fields:
- tldr: array of 3 short bullet strings (each 10-18 words max).
- summaryHtml: an HTML fragment (approx 3 short paragraphs, plain text, not lists).
- movers: an array of objects { "name": <string>, "move": <string> } e.g. { "name":"ASX200 Futures","move":"-0.2%" }.
- watchList: array of short strings describing 4 things to watch today (economic prints, earnings, rate decisions).
- headlines: an array of at most 6 objects { "title", "url", "source", "publishedAt" } taken from the input.

Input headlines (JSON array):
${JSON.stringify(articles, null, 2)}

Requirements:
- Keep language plain English, briefly Australian-flavoured (cheeky but professional).
- Keep JSON valid. Use only the keys above. Do not include any commentary or extra keys.
- If you can't find enough "movers", generate sensible high-level market movers (e.g. "Tech earnings", "RBA minutes", "AUD vs USD").
- Limit summaryHtml to about 3 paragraphs and keep each under ~40 words.

Return only the JSON object.
`;

    // Call OpenAI Chat Completion
    const openaiRes = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${OPENAI_API_KEY}`
      },
      body: JSON.stringify({
        model: 'gpt-4o-mini',
        messages: [{ role: 'user', content: prompt }],
        max_tokens: 500,
        temperature: 0.7
      })
    });

    if (!openaiRes.ok) {
      const txt = await openaiRes.text();
      return { statusCode: openaiRes.status, body: JSON.stringify({ error: 'OpenAI error', details: txt }) };
    }

    const openaiData = await openaiRes.json();
    const content = openaiData.choices?.[0]?.message?.content;

    // Try to parse JSON from the model output
    let parsed;
    try {
      parsed = JSON.parse(content);
    } catch (e) {
      // If parsing fails, try to extract JSON substring
      const m = content && content.match(/\{[\s\S]*\}/);
      if (m) {
        try { parsed = JSON.parse(m[0]); } catch (e2) { parsed = null; }
      }
    }

    // Fallback minimal structure if parsing fails
    if (!parsed) {
      const fallback = {
        tldr: articles.slice(0,3).map(a => a.title || '').filter(Boolean),
        summaryHtml: `<p>Markets - brief unavailable from AI. See top headlines below.</p>`,
        movers: [],
        watchList: [],
        headlines: articles.slice(0,6)
      };
      return { statusCode: 200, body: JSON.stringify(fallback) };
    }

    // Ensure headlines include actual article URLs (map to original list for safety)
    if (Array.isArray(parsed.headlines) && parsed.headlines.length) {
      parsed.headlines = parsed.headlines.map(h => {
        if (!h.url) {
          // try to find matching article by title
          const found = articles.find(a => a.title && h.title && a.title.includes(h.title.slice(0,20)));
          if (found) h.url = found.url;
        }
        return h;
      });
    } else {
      parsed.headlines = articles.slice(0,6);
    }

    return {
      statusCode: 200,
      body: JSON.stringify(parsed)
    };

  } catch (err) {
    console.error('morning-brief error', err);
    return { statusCode: 500, body: JSON.stringify({ error: err.message || 'Server error' }) };
  }
}