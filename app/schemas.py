# Pydantic models here describe the *shape of HTTP request/response JSON* -
# not database tables (those are the SQLAlchemy classes in app/models.py).
# FastAPI uses these to:
#   1. Parse and validate incoming JSON before your endpoint code runs
#   2. Generate the interactive docs at /docs
#   3. Shape outgoing JSON responses consistently

import re
import uuid

from pydantic import BaseModel, EmailStr, field_validator

# A permissive but real phone format check: optional leading "+", then 7-20
# digits/spaces/dashes/parentheses. Rejects obviously-wrong input (letters,
# way too short/long) without being overly strict about international formats.
_PHONE_PATTERN = re.compile(r"^\+?[0-9()\-\s]{7,20}$")


class LeadCreateRequest(BaseModel):
    """What the client must send in the POST /leads request body."""

    name: str
    phone: str
    # EmailStr validates this looks like a real email address
    # (e.g. rejects "not-an-email") using the email-validator library.
    email: EmailStr
    property_address: str
    source: str

    # A "field_validator" runs custom validation logic on top of the type
    # check pydantic already does. If it raises ValueError, FastAPI turns
    # that into a 422 Unprocessable Entity response with a clear error
    # message - the endpoint function below never even gets called.
    @field_validator("phone")
    @classmethod
    def validate_phone_format(cls, value: str) -> str:
        if not _PHONE_PATTERN.match(value.strip()):
            raise ValueError(
                "phone must be a valid phone number, e.g. '+1 415-555-0100'"
            )
        return value.strip()


class LeadCreateResponse(BaseModel):
    """What we send back after a successful POST /leads."""

    id: uuid.UUID
