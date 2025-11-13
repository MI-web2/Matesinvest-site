// netlify/functions/newsFeed.js

exports.handler = async () => {
  try {
    const apiKey = process.env.NEWSAPI_KEY;
    if (!apiKey) {
      return { statusCode: 500, body: "Missing NEWSAPI_KEY" };
    }

    const url = new URL("https://newsapi.org/v2/top-headlines");
    url.searchParams.set("country", "au");
    url.searchParams.set("category", "business");
    url.searchParams.set("pageSize", "20");

    const res = await fetch(url.toString(), {
      headers: { "X-Api-Key": apiKey },
    });

    if (!res.ok) {
      const text = await res.text();
      console.error("NewsAPI error:", text);
      return { statusCode: 500, body: "Error from NewsAPI" };
    }

    const data = await res.json();

    const articles = (data.articles || []).map((a, idx) => ({
      id: idx,
      title: a.title,
      description: a.description,
      url: a.url,
      source: a.source?.name,
      publishedAt: a.publishedAt,
    }));

    return {
      statusCode: 200,
      body: JSON.stringify({ articles }),
    };
  } catch (err) {
    console.error(err);
    return { statusCode: 500, body: "Server error" };
  }
};
