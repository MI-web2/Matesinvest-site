export async function handler(event) {
  try {
    const API_KEY = process.env.OPENAI_API_KEY;

    const body = JSON.parse(event.body);

    const prompt = `
Write a cheeky Australian Betoota-style sports summary.
Max 2 sentences. Light banter, nothing abusive.
Mention the teams and score.

Data:
Sport: ${body.sport}
League: ${body.league}
Home: ${body.home} (${body.homeScore})
Away: ${body.away} (${body.awayScore})
Status: ${body.status}
`;

    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${API_KEY}`
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages: [{ role: "user", content: prompt }],
        max_tokens: 60,
        temperature: 0.8
      })
    });

    const data = await response.json();

    return {
      statusCode: 200,
      body: JSON.stringify({ summary: data.choices[0].message.content })
    };

  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
}
