# How to Verify Session Tracking

This guide explains how to verify that session tracking is working correctly.

## Quick Check

1. **Open any tracked page** (e.g., https://matesinvest.com/)
2. **Open browser DevTools** (F12 or right-click → Inspect)
3. **Go to Network tab**
4. **Interact with the page** (scroll, click)
5. **Navigate away or close the tab**
6. Look for requests to:
   - `track-visit` (should fire immediately on page load)
   - `track-session` (should fire on page unload or every 30s)

## Detailed Verification

### 1. Check localStorage and sessionStorage

In browser console:
```javascript
// Check user ID (persists across sessions)
localStorage.getItem('mates_user_id_v1')

// Check session ID (unique per tab)
sessionStorage.getItem('mates_session_id_v1')

// Check session start time
sessionStorage.getItem('mates_session_start_v1')
```

### 2. Monitor Network Requests

Track-visit request (on page load):
```json
POST /.netlify/functions/track-visit
{
  "uid": "abc123...",
  "path": "/discover.html",
  "ts": 1767338605034
}
```

Track-session request (on page unload):
```json
POST /.netlify/functions/track-session
{
  "uid": "abc123...",
  "sessionId": "xyz789...",
  "path": "/discover.html",
  "sessionSeconds": 45,
  "isEngaged": true,
  "ts": 1767338650034
}
```

### 3. Query Upstash Redis

Using Upstash Console or CLI:

```redis
# Get all metrics for today (replace date)
HGETALL mates:analytics:day:2026-01-02

# Get specific metrics
HGET mates:analytics:day:2026-01-02 session_count
HGET mates:analytics:day:2026-01-02 session_seconds_total
HGET mates:analytics:day:2026-01-02 engaged_sessions

# Get per-path metrics
HGETALL mates:analytics:day:2026-01-02:pathstats
HGET mates:analytics:day:2026-01-02:pathstats "/discover.html|session_count"
```

### 4. Test Engagement Detection

To test if engagement is being tracked:

1. **Short visit (< 10s, no scroll, no clicks)**: Should NOT be engaged
2. **Long visit (> 10s) + scroll (> 25%)**: Should BE engaged
3. **Long visit (> 10s) + clicks (> 3)**: Should BE engaged
4. **Scroll (> 25%) + clicks (> 3)**: Should BE engaged

### 5. Test Session Continuity

1. Load a page
2. Note the session ID in sessionStorage
3. Navigate to another page on the same site
4. Check session ID - should be the SAME
5. Open a new tab to the same site
6. Check session ID - should be DIFFERENT

## Expected Results

After visiting several pages, you should see in Upstash:

```
mates:analytics:day:2026-01-02 = {
  session_count: 15,
  session_seconds_total: 3450,
  engaged_sessions: 12,
  visits: 47,
  unique_users: 15,
  ...
}
```

## Troubleshooting

### No data in Upstash
- Check that Upstash environment variables are set in Netlify
- Check browser console for errors
- Verify Network tab shows successful requests

### Session not being tracked
- Check that script is loaded: `document.querySelector('script[src="/scripts/track-session.js"]')`
- Check for JavaScript errors in console
- Verify localStorage and sessionStorage are available

### Engagement always false
- Check scroll tracking: scroll the page and see if `maxScrollPercent` increases
- Check click tracking: click elements and see if `interactionCount` increases
- Verify time tracking: session duration should increase

## Performance Impact

The tracking is designed to be lightweight:
- Event listeners use passive mode for scroll
- Data sent on unload doesn't block navigation
- Periodic updates (30s) are minimal overhead
- No external analytics libraries required

## Privacy Notes

- User IDs are randomly generated and stored locally
- No personal information is collected
- All data stays in your Upstash database
- GDPR/privacy compliant (anonymous analytics)
