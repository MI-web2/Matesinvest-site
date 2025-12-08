const fetch = (...args) => global.fetch(...args);


exports.handler = async function (event) {
  const slug = event.queryStringParameters.slug;

  if (!slug) {
    return { statusCode: 400, body: "Missing slug" };
  }

  const url =
    process.env.UPSTASH_REDIS_REST_URL + "/get/content:" + slug;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;

  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${token}` },
  });

  const data = await res.json();

  let parsed = {};
  try {
    parsed = JSON.parse(data.result);
  } catch (err) {
    console.error("JSON parse error:", err);
    return {
      statusCode: 500,
      body: "JSON parse error",
    };
  }

  return {
    statusCode: 200,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(parsed),
  };
};
