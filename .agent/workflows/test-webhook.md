---
description: Simulate Meta Messenger Banglish message bursts with valid HMAC-SHA256 signatures against the local Go server
---

# Workflow: Verify Webhook & Redis Debouncer

1. Ensure Redis, PostgreSQL, and the Go server (`go run ./cmd/server`) are running on `http://localhost:8080`.
2. Verify the Meta webhook challenge endpoint returns the exact challenge string:
   ```bash
   curl -i "http://localhost:8080/webhook?hub.mode=subscribe&hub.verify_token=${META_VERIFY_TOKEN}&hub.challenge=123456789"
   ```
3. Run the Go test suite with the race detector enabled:
   ```bash
   go test -race -v ./...
   ```
4. Simulate a rapid 3-message Banglish customer burst (`"vai"`, `"ei black hoodie ta ache?"`, `"dhakar vitore delivery charge koto?"`) signed with `META_APP_SECRET`:
   ```bash
   python3 -c '
   import hmac, hashlib, json, time, os, urllib.request

   secret = os.environ["META_APP_SECRET"].encode()
   msgs = [
       ("mid.test.1", "vai"),
       ("mid.test.2", "ei black hoodie ta ache?"),
       ("mid.test.3", "dhakar vitore delivery charge koto?")
   ]

   for mid, text in msgs:
       body = json.dumps({
           "object": "page",
           "entry": [{
               "id": "PAGE_123",
               "time": int(time.time() * 1000),
               "messaging": [{
                   "sender": {"id": "USER_TEST_001"},
                   "recipient": {"id": "PAGE_123"},
                   "timestamp": int(time.time() * 1000),
                   "message": {"mid": mid, "text": text}
               }]
           }]
       }).encode()
       sig = "sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest()
       req = urllib.request.Request(
           "http://localhost:8080/webhook",
           data=body,
           headers={"Content-Type": "application/json", "X-Hub-Signature-256": sig},
           method="POST"
       )
       with urllib.request.urlopen(req) as resp:
           print(mid, resp.status)
       time.sleep(0.6)
   '
   ```
5. Inspect the JSON logs from `cmd/server` and confirm:
   - All 3 `POST /webhook` requests returned `200 OK` in `< 50ms`.
   - Only **one** `processing debounced turn` log entry fired after 3 seconds with `msg_count: 3`.
   - The agent responded in natural Bangladeshi Bangla (*চলিত ভাষা*).
