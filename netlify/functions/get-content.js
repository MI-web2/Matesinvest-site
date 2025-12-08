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

    const data = await res.json();

    if (!data.result) {
      return {
        statusCode: 404,
        body: "Not found",
      };
    }

    // data.result is already a JSON string like:
    // {"title":"...","meta_description":"...","content_html":"..."}
    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: data.result,
    };
  } catch (err) {
    console.error("Error fetching from Upstash", err);
    return {
      statusCode: 500,
      body: "Server error",
    };
  }
};
