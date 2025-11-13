// netlify/functions/newsFeed.js

exports.handler = async () => {
  try {
    const apiKey = process.env.NEWSAPI_KEY;
    if (!apiKey) {
      return { statusCode: 500, body: "Missing NEWSAPI_KEY" };
    }

    // First attempt: AU + business
    const primaryUrl = new URL("https://newsapi.org/v2/top-headlines");
    primaryUrl.searchParams.set("country", "au");
    primaryUrl.searchParams.set("category", "business");
    primaryUrl.searchParams.set("pageSize", "20");
    primaryUrl.searchParams.set("apiKey", apiKey);

    let articles = await fetchHeadlines(primaryUrl);

    // If nothing came back, try AU with no category filter
    if (!articles.length) {
      console.warn("No AU business headlines, retrying with country=au only");

      const fallbackUrl = new URL("https://newsapi.org/v2/top-headlines");
      fallbackUrl.searchParams.set("country", "au");
      fallbackUrl.searchParams.set("pageSize", "20");
      fallbackUrl.searchParams.set("apiKey", apiKey);

      articles = await fetchHeadlines(fallbackUrl);
    }

    // If we STILL have nothing, use our hard-coded demo ones
    if (!articles.length) {
      console.warn("Still zero headlines from NewsAPI, using hard-coded demos.");
      articles = getFallbackArticles("No live headlines returned from NewsAPI");
    }

    const mapped = articles.map((a, idx) => ({
      id: idx,
      title: a.title,
      description: a.description,
      url: a.url || "https://matesinvest.com",
      source: (a.source && a.source.name) || "MatesInvest",
      publishedAt: a.publishedAt || new Date().toISOString(),
    }));

    return {
      statusCode: 200,
      body: JSON.stringify({ articles: mapped }),
    };
  } catch (err) {
    console.error("newsFeed function error:", err);
    const fallbackArticles = getFallbackArticles("Unexpected server error");
    return {
      statusCode: 200,
      body: JSON.stringify({ articles: fallbackArticles }),
    };
  }
};

async function fetchHeadlines(url) {
  try {
    const res = await fetch(url.toString());
    if (!res.ok) {
      const text = await res.text();
      console.error("NewsAPI HTTP error:", text);
      return [];
    }

    const data = await res.json();

    if (data.status && data.status !== "ok") {
      console.error("NewsAPI logical error:", data);
      return [];
    }

    return Array.isArray(data.articles) ? data.articles : [];
  } catch (e) {
    console.error("Error fetching headlines:", e);
    return [];
  }
}

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
