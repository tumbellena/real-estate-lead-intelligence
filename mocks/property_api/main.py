# Mock third-party "property data" provider. Real services like this
# (Zillow, ATTOM, CoreLogic, etc.) charge per lookup and are unreliable in
# the ways MODE simulates below - this lets us build/test the app's
# enrichment logic without hitting a real API or paying for it.
import asyncio
import os
import random
from datetime import date, timedelta

from fastapi import FastAPI, HTTPException

# Read once at startup. Change it via `docker compose up -d --build` after
# editing docker-compose.yml, or `docker compose exec property_api ...` env
# tricks - simplest is just changing MODE in docker-compose.yml and restarting.
MODE = os.getenv("MODE", "healthy")

app = FastAPI(title="Mock Property API")


async def apply_mode() -> None:
    """Simulates the failure modes a real third-party API can exhibit."""
    if MODE == "rate_limited":
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded",
            headers={"Retry-After": "5"},
        )
    if MODE == "broken":
        raise HTTPException(status_code=500, detail="Internal server error")
    if MODE == "slow":
        await asyncio.sleep(10)


@app.get("/health")
def health_check():
    return {"status": "ok", "mode": MODE}


@app.get("/property")
async def get_property(address: str):
    await apply_mode()

    # Seed the RNG from the address so the same address always returns the
    # same fake data, instead of a new random property on every call.
    rng = random.Random(address.strip().lower())

    sale_days_ago = rng.randint(30, 5000)

    return {
        "address": address,
        "estimated_value": rng.randint(150_000, 950_000),
        "bedrooms": rng.randint(2, 5),
        "bathrooms": rng.choice([1, 1.5, 2, 2.5, 3, 3.5]),
        "square_footage": rng.randint(900, 4200),
        "year_built": rng.randint(1950, 2022),
        "last_sale_date": (date.today() - timedelta(days=sale_days_ago)).isoformat(),
    }
