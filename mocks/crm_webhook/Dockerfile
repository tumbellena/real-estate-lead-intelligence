FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8003

# log_config=None: see the equivalent comment in the root Dockerfile - lets
# uvicorn's own request logs flow through the same structlog JSON pipeline
# main.py configures, instead of being silently exempted from it.
CMD ["python", "-c", "import uvicorn; uvicorn.run('main:app', host='0.0.0.0', port=8003, log_config=None)"]
