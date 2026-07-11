# Mock third-party services

Fake stand-ins for external vendors the real app would eventually call
during lead enrichment. Each one is a tiny standalone FastAPI app in its
own folder with its own `Dockerfile`, run as its own container by
`docker-compose.yml`.

| Service       | Port | Endpoint                     | Simulates                          |
|---------------|------|-------------------------------|-------------------------------------|
| `property_api`| 8001 | `GET /property?address=X`     | A property data vendor (Zillow/ATTOM-style) |
| `phone_api`   | 8002 | `GET /validate?phone=X`       | A phone validation vendor (Twilio Lookup-style) |
| `crm_webhook` | 8003 | `POST /leads`                 | A CRM's inbound webhook (logs whatever it receives) |

All three also expose `GET /health`.

## MODE

Every mock reads a `MODE` environment variable at startup that controls
how it behaves, so the main app's error handling can be exercised without
a real vendor ever being unreliable on cue:

| MODE           | Behavior                                      |
|-----------------|------------------------------------------------|
| `healthy` (default) | Responds normally                         |
| `rate_limited`  | Returns `429` with a `Retry-After: 5` header  |
| `broken`        | Returns `500`                                 |
| `slow`          | Waits 10 seconds, then responds normally      |

Set it per-service via `docker-compose.yml` (`PROPERTY_API_MODE`,
`PHONE_API_MODE`, `CRM_WEBHOOK_MODE` env vars, e.g. in `.env`), then
restart that container:

```bash
PROPERTY_API_MODE=rate_limited docker compose up -d property_api
```

## Data is fake but deterministic

`property_api` and `phone_api` seed their random generator from the input
(`address` / `phone`), so the same input always returns the same fake
result - useful for writing repeatable tests against them.

## Not yet wired into the main app

`app/main.py` doesn't call any of these yet - they're available on the
shared Docker network at `http://property_api:8001`, `http://phone_api:8002`,
and `http://crm_webhook:8003` for whenever lead enrichment/CRM-push logic
is added.
