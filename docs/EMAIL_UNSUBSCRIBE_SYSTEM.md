# Email Unsubscribe System Documentation

## Overview
This document explains how the email unsubscribe functionality works in the MatesInvest site, where unsubscribed emails are stored, and how to access the data in Upstash Redis.

## How the Unsubscribe Function Works

### Endpoint Details
- **URL**: `/.netlify/functions/unsubscribe`
- **Method**: GET
- **Query Parameter**: `?email=user@example.com`
- **Returns**: HTML confirmation page

### Process Flow
1. **Email Validation**: The function receives an email address via query parameter and validates:
   - Email is present and is a string
   - Email format is valid (using regex: `/^[^\s@]+@[^\s@]+\.[^\s@]+$/`)
   - Email is trimmed and converted to lowercase

2. **Removal from Lists**: The function removes the email from **two Redis sets** in parallel:
   - Uses Redis `SREM` (Set Remove) command
   - Removes from both subscriber lists simultaneously using `Promise.all()`
   - Request timeout is set to 7 seconds

3. **Response**: Returns an HTML page with:
   - Success: Styled confirmation page with the unsubscribed email displayed
   - Error: Appropriate error page for validation failures or system errors

### Code Location
File: `/netlify/functions/unsubscribe.js`

## Where Unsubscribed Emails Are Stored

**Important**: Unsubscribed emails are **NOT stored in a separate list**. Instead, they are **removed** from the active subscriber lists.

### Active Subscriber Lists in Upstash Redis

The system maintains **two Redis Sets** (not lists) for active subscribers:

| Redis Key Name | Purpose | Description |
|----------------|---------|-------------|
| `email:subscribers` | Daily Updates | Main list for daily email subscribers |
| `email:subscribers-App` | App Waitlist | Subscribers interested in the upcoming app |

When a user unsubscribes, their email is **removed** from both sets using the Redis `SREM` command.

### To View Unsubscribed Emails

Since unsubscribed emails are removed (not stored separately), you would need to:

1. **Compare with your original subscriber list** - If you have a historical backup of subscribers, you can compare it with the current lists to identify who unsubscribed
2. **Implement a separate unsubscribe tracking list** - If you need to track who has unsubscribed, you would need to add this functionality to also add emails to an `email:unsubscribed` list when they unsubscribe

### Current Behavior
- Email exists in lists → User is subscribed
- Email does NOT exist in lists → User is either unsubscribed OR never subscribed

## Upstash Integration

### Configuration
The function uses **Upstash Redis** as the data store with the following environment variables:
- `UPSTASH_REDIS_REST_URL` - The Upstash Redis REST API URL
- `UPSTASH_REDIS_REST_TOKEN` - Authentication token for Upstash

### Redis Commands Used
```javascript
// Remove email from a set
SREM email:subscribers user@example.com
SREM email:subscribers-App user@example.com
```

### List Names Summary
**Active Subscriber Lists** (these are Redis Sets):
1. **`email:subscribers`** - Daily updates subscriber list
2. **`email:subscribers-App`** - App waitlist subscribers

**Note**: There is currently NO dedicated unsubscribe list. Emails are simply removed from the active lists.

## Subscription System (Context)

### How Users Subscribe
File: `/netlify/functions/subscribe.js`

The subscribe function adds emails to the appropriate lists based on the source:

1. **Regular Subscriptions** (no source or non-app source):
   - Added to `email:subscribers` (daily updates)

2. **App Signups** (sources: "meta-social-coming-soon", "app-early-access", "social-investing"):
   - Always added to `email:subscribers-App`
   - Optionally added to `email:subscribers` if `daily_updates: true`

3. **User IDs**: Each subscriber is also assigned a unique sequential ID in format `MI0000001`:
   - Mapping: `email:id:{email}` → `MI0000001`
   - Reverse: `id:email:{MI0000001}` → `email`
   - Counter: `user:id:counter` (incremented for new IDs)

## How to Access Subscriber Lists in Upstash

### View All Active Subscribers

To view the current subscriber lists in Upstash Redis:

1. **Log into Upstash Console**: https://console.upstash.com/
2. **Select your Redis database**
3. **Use the Data Browser** or **CLI** to query:

```redis
# Get all emails in daily subscribers list
SMEMBERS email:subscribers

# Get all emails in app waitlist
SMEMBERS email:subscribers-App

# Get count of subscribers
SCARD email:subscribers
SCARD email:subscribers-App

# Check if a specific email is subscribed
SISMEMBER email:subscribers user@example.com
SISMEMBER email:subscribers-App user@example.com
```

### Export Subscribers
There's also a function to export subscribers:
- File: `/netlify/functions/export-subscribers.js`

## Recommendations

If you need to track unsubscribed emails separately, consider:

1. **Add an unsubscribe tracking list**:
   - Modify `unsubscribe.js` to also add emails to `email:unsubscribed` set
   - Use `SADD email:unsubscribed {email}` before removing from active lists

2. **Add timestamp tracking**:
   - Store when users unsubscribe: `unsubscribe:{email}` → timestamp
   - Useful for analytics and compliance

3. **Audit logging**:
   - Log unsubscribe events to a separate analytics system
   - Track unsubscribe reasons if you add a feedback form

## Example Unsubscribe Links in Emails

Current email templates should include unsubscribe links like:
```html
<a href="https://matesinvest.com/.netlify/functions/unsubscribe?email={{EMAIL}}">
  Unsubscribe
</a>
```

## Security & Privacy Notes

- Emails are validated and sanitized (trimmed, lowercased)
- HTML output properly escapes user input to prevent XSS
- No authentication required for unsubscribe (compliance best practice)
- CORS enabled for cross-origin requests
- Request timeout prevents hanging requests (7 seconds)
