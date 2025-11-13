// netlify/functions/newsFeed.js

exports.handler = async () => {
  try {
    const apiKey = process.env.NEWSAPI_KEY;
    if (!apiKey) {
      return { statusCode: 500, body: "Missing NEWSAPI_KEY" };
    }

    const url = new URL("https://newsapi.org/v2/top-headlines");
    url.searchParams.set("country", "us");       // <-- dev plan supports US
    url.searchParams.set("category", "business");
    url.searchParams.set("pageSize", "20");
    url.searchParams.set("apiKey", apiKey);

    const res = await fetch(url.toString());

    if (!res.ok) {
      const text = await res.text();
      console.error("NewsAPI HTTP error:", text);
      const fallbackArticles = getFallbackArticles("HTTP error: " + text);
      return {
        statusCode: 200,
        body: JSON.stringify({ articles: fallbackArticles }),
      };
    }

    const data = await res.json();

    if (data.status && data.status !== "ok") {
      console.error("NewsAPI logical error:", data);
      const fallbackArticles = getFallbackArticles(
        "NewsAPI error: " + (data.message || data.code)
      );
      return {
        statusCode: 200,
        body: JSON.stringify({ articles: fallbackArticles }),
      };
    }

    let articles = Array.isArray(data.articles) ? data.articles : [];

    if (!articles.length) {
      console.warn("NewsAPI returned zero articles, using fallback.");
      articles = getFallbackArticles("No live headlines returned");
    }

    const mapped = articles.map((a, idx) => ({
      id: idx,
      title: a.title,
      description: a.description,
      url: a.url || "https://matesinvest.com",
      source: (a.source && a.source.name) || "NewsAPI",
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
