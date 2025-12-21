const fetch = (...args) => global.fetch(...args);

exports.handler = async function () {
  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

  const res = await fetch(
    `${UPSTASH_URL}/smembers/email:subscribers`,
    { headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` } }
  );

  const j = await res.json();
  const emails = Array.isArray(j.result) ? j.result : [];

  const csv = ["email", ...emails].join("\n");

  return {
    statusCode: 200,
    headers: {
      "content-type": "text/csv",
      "content-disposition": "attachment; filename=subscribers.csv",
    },
    body: csv,
  };
};
