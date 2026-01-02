// netlify/functions/domain-suburb-stats-test.js
const fetch = (...args) => global.fetch(...args);

async function getDomainToken() {
  const clientId = process.env.DOMAIN_CLIENT_ID;
  const clientSecret = process.env.DOMAIN_CLIENT_SECRET;

  const tokenUrl = "https://auth.domain.com.au/v1/connect/token";
  const body = new URLSearchParams({
    grant_type: "client_credentials",
    // If your project later gets more scopes, they’ll appear automatically in token response.
  });

  const res = await fetch(tokenUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      Authorization: "Basic " + Buffer.from(`${clientId}:${clientSecret}`).toString("base64"),
    },
    body: body.toString(),
  });

  const json = await res.json();
  if (!res.ok) throw new Error(`Token error ${res.status}: ${JSON.stringify(json)}`);
  return json.access_token;
}

exports.handler = async (event) => {
  try {
    const token = await getDomainToken();
    const apiKey = process.env.DOMAIN_API_KEY; // may be optional; we include if present

    // Try a known suburb as a “can we access stats?” test
    // V2 format commonly looks like: /v2/suburbPerformanceStatistics/{state}/{suburb}/{postcode}
    const url =
      "https://api.domain.com.au/v2/suburbPerformanceStatistics/qld/brisbane%20city/4000";

    const headers = {
      Authorization: `Bearer ${token}`,
      Accept: "application/json",
    };
    if (apiKey) headers["X-Api-Key"] = apiKey;

    const res = await fetch(url, { headers });
    const text = await res.text();

    let data;
    try {
      data = JSON.parse(text);
    } catch {
      data = { raw: text };
    }

    // If it works, return just a small preview (don’t spam the browser)
    if (res.ok) {
      const preview = Array.isArray(data) ? data.slice(0, 3) : data;
      return {
        statusCode: 200,
        body: JSON.stringify(
          { ok: true, status: res.status, preview },
          null,
          2
        ),
      };
    }

    return {
      statusCode: res.status,
      body: JSON.stringify({ ok: false, status: res.status, error: data }, null, 2),
    };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ ok: false, error: String(err) }) };
  }
};
