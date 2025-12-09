const fetch = (...args) => global.fetch(...args);

exports.handler = async function () {
  const baseUrl = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;

  try {
    // 1. Get all content:* keys from Upstash
    const keysRes = await fetch(`${baseUrl}/keys/content:*`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const keysData = await keysRes.json();
    let keys = keysData.result || [];

    // Remove the pointer key if you're using content:latest
    keys = keys.filter((k) => k !== "content:latest");

    const articles = [];

    // 2. For each key, fetch the article and pull out title/excerpt/slug
    for (const key of keys) {
      try {
        const getRes = await fetch(`${baseUrl}/get/${encodeURIComponent(key)}`, {
          headers: { Authorization: `Bearer ${token}` },
        });
        const getData = await getRes.json();
        if (!getData.result) continue;

        // Same unwrapping logic as get-content.js
        let article;
        const first = JSON.parse(getData.result);

        if (first && typeof first === "object" && first.title) {
          article = first;
        } else if (
          first &&
          typeof first === "object" &&
          "" in first &&
          typeof first[""] === "string"
        ) {
          article = JSON.parse(first[""]);
        } else {
          continue;
        }

        articles.push({
          slug: key.replace("content:", ""),
          title: article.title || "(Untitled article)",
          excerpt: article.excerpt || "",
          canonical_url: article.canonical_url || "",
        });
      } catch (err) {
        console.error("Error loading article for key", key, err);
        continue;
      }
    }

    // Optional: sort alphabetically by title
    articles.sort((a, b) => a.title.localeCompare(b.title));

    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ articles }),
    };
  } catch (err) {
    console.error("Error listing content keys", err);
    return {
      statusCode: 500,
      body: "Server error",
    };
  }
};
