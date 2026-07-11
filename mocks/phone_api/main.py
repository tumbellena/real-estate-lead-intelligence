# Mock third-party phone validation provider (like Twilio Lookup or
# NumVerify). Real services like this get rate-limited, go down, and
# sometimes just hang - MODE simulates each of those so we can build/test
# retry and timeout handling without depending on a real vendor.
import asyncio
import os
import random

from fastapi import FastAPI, HTTPException

MODE = os.getenv("MODE", "healthy")

app = FastAPI(title="Mock Phone Validation API")

_CARRIERS = ["Verizon", "AT&T", "T-Mobile", "US Cellular"]
_TIMEZONES = ["America/New_York", "America/Chicago", "America/Denver", "America/Los_Angeles"]


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


@app.get("/validate")
async def validate_phone(phone: str):
    await apply_mode()

    # Seed the RNG from the phone number so the same number always returns
    # the same fake result, instead of a new random answer on every call.
    rng = random.Random(phone.strip())

    return {
        "phone": phone,
        "is_mobile": rng.random() < 0.7,
        "carrier": rng.choice(_CARRIERS),
        "is_disconnected": rng.random() < 0.05,
        "timezone": rng.choice(_TIMEZONES),
    }
