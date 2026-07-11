# This module sets up everything needed to talk to Postgres:
# - an "engine" (manages a pool of real network connections)
# - a session factory (hands out one unit-of-work session per request)
# - a `Base` class that all our table models inherit from

import os

from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

# Reads the .env file (if present) and copies its values into the process's
# environment variables, so os.getenv() below can see DATABASE_URL.
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

# The engine manages a pool of connections to Postgres and knows how to
# speak SQL to it asynchronously (via the asyncpg driver named in the URL).
# echo=False keeps raw SQL statements out of the logs; set True when debugging.
engine = create_async_engine(DATABASE_URL, echo=False)

# Calling AsyncSessionLocal() creates a new database session bound to our
# engine. Each incoming request should get its own session - see get_db().
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    """
    Every ORM model (see app/models.py) inherits from this class.
    SQLAlchemy uses it to collect all table definitions into `Base.metadata`,
    which Alembic reads to figure out what the database schema should look like.
    """

    pass


async def get_db():
    """
    A FastAPI "dependency" - a function FastAPI calls on your behalf before
    running a route, to provide it something it needs. Here, it hands the
    route a database session, and guarantees the session is closed again
    afterwards even if the route raises an error.

    Usage in a route:
        @app.get("/leads")
        async def list_leads(db: AsyncSession = Depends(get_db)):
            ...
    """
    async with AsyncSessionLocal() as session:
        yield session
