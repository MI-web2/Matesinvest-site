export async function handler() {
  const API_KEY = process.env.SPORTSDB_API_KEY;
  const today = new Date().toISOString().split("T")[0];

  const url = `https://www.thesportsdb.com/api/v1/json/${API_KEY}/eventsday.php?d=${today}`;

  const response = await fetch(url);
  const data = await response.json();

  const events = data.events || [];

  // Step 2: SIMPLE FILTERING
  const allowedLeagues = [
    "NRL",
    "AFL",
    "NBA",
    "English Premier League",
    "Australian A-League",
    "NBL",
    "Big Bash League"
  ];

  const filtered = events.filter(event =>
    allowedLeagues.includes(event.strLeague)
  );

  return {
    statusCode: 200,
    body: JSON.stringify(filtered, null, 2),
  };
}
