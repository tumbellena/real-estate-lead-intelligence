# Start from an official, lightweight Python image.
# "slim" means it has the essentials but isn't bloated with extra OS packages.
FROM python:3.12-slim

# Set the working directory inside the container.
# Every command below (COPY, RUN, CMD) runs relative to this path.
WORKDIR /app

# Copy just the requirements file first (not the whole project yet).
# This is a Docker caching trick: as long as requirements.txt doesn't change,
# Docker reuses the cached "pip install" layer instead of re-running it,
# which makes rebuilds much faster.
COPY requirements.txt .

# Install our Python dependencies.
# --no-cache-dir keeps the image smaller by not storing pip's download cache.
RUN pip install --no-cache-dir -r requirements.txt

# Now copy the rest of the application code into the container.
COPY . .

# Document which port the app listens on (informational; doesn't actually
# publish the port - that happens in docker-compose.yml).
EXPOSE 8000

# The command that runs when the container starts.
# Runs uvicorn via a Python one-liner instead of the plain `uvicorn` CLI so
# we can pass log_config=None: uvicorn's default behavior is to call
# logging.config.dictConfig() on startup, which assigns its "uvicorn" and
# "uvicorn.access" loggers their own handlers with propagate=False - that
# silently undoes the structlog JSON setup app/main.py configures at import
# time (via app/logging_config.py) for exactly those two logger names,
# leaving uvicorn's own request logs as plain text next to our JSON lines.
# log_config=None skips that step, so those loggers fall back to Python's
# default (no handler of their own, propagate=True) and flow through the
# same root handler and JSON renderer as everything else.
CMD ["python", "-c", "import uvicorn; uvicorn.run('app.main:app', host='0.0.0.0', port=8000, log_config=None)"]
