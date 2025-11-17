// netlify/functions/morning-brief.js
// Faster morning-brief: single, low-latency OpenAI call with short timeout and small token budget.
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

    // Build NewsAPI query
    let q = 'market OR stocks OR ASX OR S&P OR NASDAQ OR futures OR inflation OR rates';
    if (region === 'au') {
      q = 'ASX OR Australia OR ASX200 OR "Australian share" OR market OR stocks';
    } else if (region === 'us') {
      q = 'Wall Street OR S&P OR NASDAQ OR US economy OR stocks OR futures';
    } else {
      q = 'markets OR stocks OR global markets OR futures';
    }

    const newsUrl = `https://newsapi.org/v2/everything?q=${encodeURIComponent(q)}&language=en&pageSize=12&sortBy=publishedAt`;
    const newsRes = await fetch(newsUrl, { headers: { 'X-Api-Key': NEWSAPI_KEY } });

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

    const prompt = `
You are a market briefing assistant writing "Morning Briefing" for retail investors.
Produce a concise briefing using the input headlines below. Output JSON ONLY with these fields:
- tldr: array of 3 short bullet strings.
- summaryHtml: an HTML fragment (approx 2-3 short paragraphs).
- movers: array of objects { "name", "move" }.
- watchList: array of 3-5 short items to watch today.
- headlines: up to 6 headline objects { "title","url","source","publishedAt" } from the input.

Input headlines:
${JSON.stringify(articles, null, 2)}

Return only the JSON object, no surrounding text or commentary.
`.trim();

    // Helper: one-shot OpenAI call with short timeout
    async function callOpenAITimeout(messages, timeoutMs = 8000, maxTokens = 350, temperature = 0.0) {
      const controller = new AbortController();
      const id = setTimeout(() => controller.abort(), timeoutMs);
      try {
        const res = await fetch('https://api.openai.com/v1/chat/completions', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${OPENAI_API_KEY}`
          },
          body: JSON.stringify({
            model: 'gpt-4o-mini',
            messages,
            max_tokens: maxTokens,
            temperature
          }),
          signal: controller.signal
        });
        clearTimeout(id);
        if (!res.ok) {
          const txt = await res.text();
          const err = new Error('OpenAI error: ' + txt);
          err.status = res.status;
          throw err;
        }
        return res.json();
      } catch (err) {
        clearTimeout(id);
        if (err.name === 'AbortError') throw new Error('OpenAI request timeout');
        throw err;
      }
    }

    // Small robust extractor for JSON substring
    function extractBalancedJSON(text) {
      if (!text || typeof text !== 'string') return null;
      const first = text.indexOf('{');
      if (first === -1) return null;
      let inString = false;
      let escape = false;
      let depth = 0;
      let start = -1;
      for (let i = first; i < text.length; i++) {
        const ch = text[i];
        if (!inString) {
          if (ch === '{') {
            if (start === -1) start = i;
            depth++;
          } else if (ch === '}') {
            depth--;
            if (depth === 0 && start !== -1) {
              return text.slice(start, i + 1);
            }
          }
          if (ch === '"') {
            inString = true;
            escape = false;
          }
        } else {
          if (escape) {
            escape = false;
            continue;
          }
          if (ch === '\\') {
            escape = true;
            continue;
          }
          if (ch === '"') {
            inString = false;
          }
        }
      }
      return null;
    }

    const systemMsg = { role: 'system', content: 'You are a JSON generator. Output ONLY valid JSON matching the schema requested. No commentary.' };
    const userMsg = { role: 'user', content: prompt };

    // Try one quick call
    let parsed = null;
    let rawOutput = null;
    try {
      const first = await callOpenAITimeout([systemMsg, userMsg], 8000, 350, 0.0);
      rawOutput = first.choices?.[0]?.message?.content || '';
      console.log('OpenAI raw output (single):', rawOutput);
      try {
        parsed = JSON.parse(rawOutput.trim());
      } catch (e) {
        const candidate = extractBalancedJSON(rawOutput.trim());
        if (candidate) {
          try { parsed = JSON.parse(candidate); } catch (e2) { parsed = null; }
        }
      }
    } catch (err) {
      console.warn('OpenAI call failed/timeout:', err.message || err);
    }

    // If parsing succeeded, ensure headlines have URLs & return
    if (parsed) {
      if (Array.isArray(parsed.headlines) && parsed.headlines.length) {
        parsed.headlines = parsed.headlines.map(h => {
          if (!h.url) {
            const found = articles.find(a => a.title && h.title && a.title.includes(h.title.slice(0, 20)));
            if (found) h.url = found.url;
          }
          return h;
        });
      } else {
        parsed.headlines = articles.slice(0, 6);
      }
      console.log('Returning parsed morning brief (fast path).');
      return { statusCode: 200, body: JSON.stringify(parsed) };
    }

    // Fast fallback: return headlines-only brief immediately (so UI isn't blocked)
    const fallback = {
      tldr: articles.slice(0, 3).map(a => a.title || '').filter(Boolean),
      summaryHtml: `<p>Markets - brief unavailable from AI. See top headlines below.</p>`,
      movers: [],
      watchList: [],
      headlines: articles.slice(0, 6),
      debugRaw: rawOutput // dev visibility if you want
    };
    console.log('Returning fast fallback morning brief.');
    return { statusCode: 200, body: JSON.stringify(fallback) };

  } catch (err) {
    console.error('morning-brief error', err);
    return { statusCode: 500, body: JSON.stringify({ error: err.message || 'Server error' }) };
  }
}