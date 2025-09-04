# example_app.py
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
import redis.asyncio as aioredis
from sqlalchemy import select

from mycachelib import CachedAsyncSession
from models import User, Base


async def main():
    engine = create_async_engine("sqlite+aiosqlite:///./test.db", echo=False)
    redis_client = aioredis.from_url("redis://localhost:6379/0")

    # Ensure tables exist (safe to call repeatedly)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    SessionLocal = sessionmaker(
        engine,
        expire_on_commit=False,
        class_=CachedAsyncSession,
        redis_client=redis_client,
        cache_prefix="sqlcache",
        default_ttl=120,
    )

    async with SessionLocal() as session:
        # Seed a couple of users if table is empty
        exists_payload = await session.execute(select(User.id).limit(1))
        if not exists_payload:
            session.add_all([
                User(username="alice", email="a@example.com", is_active=True),
                User(username="bob", email="b@example.com", is_active=False),
                User(username="carol", email="c@example.com", is_active=True),
            ])
            await session.commit()

        # Use SQLAlchemy expression for boolean comparison
        stmt = select(User).where(User.is_active.is_(True))

        # First call should hit DB and cache the payload (mycachelib prints the source)
        payload1 = await session.execute(stmt)
        print(
            "First query (active users) - from cache:" if session._last_from_cache else "First query (active users) - from DB:",
            payload1,
        )

        # Second call should return from cache
        payload2 = await session.execute(stmt)
        print(
            "Second query - from cache:" if session._last_from_cache else "Second query - from DB:",
            payload2,
        )


asyncio.run(main())
