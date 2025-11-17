// netlify/functions/morning-brief.js
// Morning brief with OpenAI timeout and quick fallback
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
- Keep JSON strictly valid. Use only the keys above. Do not include any commentary, explanation, or extra keys.
- Do NOT include markdown/code fences or any leading/trailing text.
- If you can't find enough "movers", generate sensible high-level market movers (e.g. "Tech earnings", "RBA minutes", "AUD vs USD").
- Limit summaryHtml to about 3 paragraphs and keep each under ~40 words.

Return ONLY the JSON object (no commentary).
`.trim();

    // Helper: call OpenAI with AbortController timeout
    async function callOpenAIWithTimeout(messages, temperature = 0.2, timeoutMs = 15000, maxTokens = 500) {
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
        if (err.name === 'AbortError') {
          throw new Error('OpenAI request timeout');
        }
        throw err;
      }
    }

    // Balanced JSON extractor (same robust approach)
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

    const systemMsg = { role: 'system', content: 'You are a JSON generator. Output ONLY valid JSON matching the schema requested. Do not include any explanation, commentary, or code fences.' };
    const userMsg = { role: 'user', content: prompt };

    // Try first with timeout
    let parsed = null;
    let modelOutputs = { first: null, retry: null };

    try {
      const firstOpenai = await callOpenAIWithTimeout([systemMsg, userMsg], 0.2, 15000, 500);
      const firstContent = firstOpenai.choices?.[0]?.message?.content || '';
      modelOutputs.first = firstContent;
      console.log('OpenAI raw output (first):', firstContent);

      try {
        parsed = JSON.parse(firstContent.trim());
      } catch (e1) {
        const candidate = extractBalancedJSON(firstContent.trim());
        if (candidate) {
          try { parsed = JSON.parse(candidate); } catch (e2) { parsed = null; }
        }
      }
    } catch (err) {
      console.warn('First OpenAI call failed or timed out:', err.message || err);
    }

    // Retry once if parse failed
    if (!parsed) {
      try {
        console.log('First parse failed or timed out, retrying with stricter prompt and 15s timeout...');
        const retryMsg = {
          role: 'user',
          content: `The previous response was not valid JSON. Please return ONLY the JSON object matching the schema (no commentary, no code fences). Here is the same input again:\n\n${prompt}`
        };
        const retryOpenai = await callOpenAIWithTimeout([systemMsg, retryMsg], 0.0, 15000, 500);
        const retryContent = retryOpenai.choices?.[0]?.message?.content || '';
        modelOutputs.retry = retryContent;
        console.log('OpenAI raw output (retry):', retryContent);

        try {
          parsed = JSON.parse(retryContent.trim());
        } catch (e3) {
          const candidate2 = extractBalancedJSON(retryContent.trim());
          if (candidate2) {
            try { parsed = JSON.parse(candidate2); } catch (e4) { parsed = null; }
          }
        }
      } catch (retryErr) {
        console.warn('Retry OpenAI call failed or timed out:', retryErr.message || retryErr);
      }
    }

    // If still not parsed, return fast fallback (headlines) so UI doesn't block
    if (!parsed) {
      const fallback = {
        tldr: articles.slice(0, 3).map(a => a.title || '').filter(Boolean),
        summaryHtml: `<p>Markets - brief unavailable from AI. See top headlines below.</p>`,
        movers: [],
        watchList: [],
        headlines: articles.slice(0, 6),
        modelOutputFirst: modelOutputs.first,
        modelOutputRetry: modelOutputs.retry
      };
      console.log('Returning fallback morning brief (AI failed or timed out).');
      console.log('Fallback preview:', JSON.stringify({ tldr: fallback.tldr, headlines: fallback.headlines.length }, null, 2));
      return { statusCode: 200, body: JSON.stringify(fallback) };
    }

    // Ensure headlines include URLs where possible
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

    // Log a short preview of the outgoing payload and return
    try {
      const preview = {
        tldrCount: Array.isArray(parsed.tldr) ? parsed.tldr.length : 0,
        headlinesCount: Array.isArray(parsed.headlines) ? parsed.headlines.length : 0
      };
      console.log('Returning parsed morning brief preview:', preview);
    } catch (e) {
      // ignore logging errors
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