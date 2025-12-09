const fetch = (...args) => global.fetch(...args);

exports.handler = async function (event) {
  const slug = event.queryStringParameters.slug;

  if (!slug) {
    return { statusCode: 400, body: "Missing slug" };
  }

  const url =
    process.env.UPSTASH_REDIS_REST_URL + "/get/content:" + slug;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;

  try {
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${token}` },
    });

    const data = await res.json(); // { result: '..."' }

    if (!data.result) {
      return {
        statusCode: 404,
        body: "Not found",
      };
    }

    let article;

    try {
      // First parse the string returned by Upstash
      const first = JSON.parse(data.result);

      // Case 1: already the final article object
      if (first && typeof first === "object" && first.title) {
        article = first;
      }
      // Case 2: wrapper like { "": "{...article json...}" }
      else if (
        first &&
        typeof first === "object" &&
        "" in first &&
        typeof first[""] === "string"
      ) {
        article = JSON.parse(first[""]);
      } else {
        throw new Error("Unexpected article shape from Upstash");
      }
    } catch (e) {
      console.error("JSON parse error:", e);
      return {
        statusCode: 500,
        body: "JSON parse error",
      };
    }

    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(article),
    };
  } catch (err) {
    console.error("Error fetching from Upstash", err);
    return {
      statusCode: 500,
      body: "Server error",
    };
  }
};
