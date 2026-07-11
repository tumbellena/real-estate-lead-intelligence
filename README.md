# Real Estate Lead Intelligence v2

A FastAPI backend with a PostgreSQL database, run via Docker Compose.

## Project structure

```
.
├── app/
│   ├── __init__.py     # marks app/ as a Python package
│   └── main.py          # FastAPI application + /health endpoint
├── requirements.txt      # Python dependencies
├── Dockerfile            # builds the API's container image
├── docker-compose.yml    # runs the API + Postgres together
├── .env.example          # template for required environment variables
└── .env                  # your real secrets (create this yourself, git-ignored)
```

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
  (this includes both `docker` and `docker compose`)

## Setup

1. Copy the example environment file and fill in real values (the defaults work fine for local development):

   ```bash
   cp .env.example .env
   ```

2. Build and start both containers (the API and the database):

   ```bash
   docker compose up --build
   ```

   The first run will:
   - Build the API image from the `Dockerfile`
   - Pull the official `postgres:16` image
   - Start both containers on a shared network

3. Once you see logs indicating uvicorn is running, check the health endpoint:

   ```bash
   curl http://localhost:8000/health
   ```

   You should get back:

   ```json
   {"status": "ok"}
   ```

   You can also open interactive API docs in your browser at:
   http://localhost:8000/docs

## Common commands

| Task                                  | Command                          |
|----------------------------------------|-----------------------------------|
| Start everything (foreground, see logs) | `docker compose up`              |
| Start everything (background)           | `docker compose up -d`           |
| Rebuild after changing dependencies     | `docker compose up --build`      |
| Stop everything                         | `docker compose down`            |
| Stop and delete database data too       | `docker compose down -v`         |
| View logs                               | `docker compose logs -f`         |

## Database migrations (Alembic)

Table definitions live in `app/models.py`. Whenever you add or change a
model, generate and apply a migration like this:

```bash
# 1. Generate a migration by diffing app/models.py against the live database
docker compose exec api alembic revision --autogenerate -m "describe your change"

# 2. Read the generated file in alembic/versions/ - autogenerate is a best
#    effort, not magic, so double check it before applying.

# 3. Apply it to the database
docker compose exec api alembic upgrade head
```

Other useful commands:

| Task                                    | Command                                   |
|-------------------------------------------|---------------------------------------------|
| See current migration version              | `docker compose exec api alembic current`  |
| See full migration history                 | `docker compose exec api alembic history`  |
| Roll back the most recent migration        | `docker compose exec api alembic downgrade -1` |

## Notes for local (non-Docker) development

If you'd rather run the API directly on your machine instead of in Docker
(you'll still need Postgres running somewhere, e.g. via `docker compose up db`):

```bash
python -m venv .venv
source .venv/bin/activate     # on Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

`--reload` restarts the server automatically whenever you edit the code.
