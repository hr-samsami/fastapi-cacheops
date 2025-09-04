# mycachelib.py
import asyncio
import contextlib
import time
import hashlib
import pickle
from typing import Any, Optional

import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Executable, Select
from sqlalchemy import event
from sqlalchemy.inspection import inspect as sa_inspect


def make_cache_key(statement: Executable, params: dict, prefix: str = "sqlcache") -> str:
    """Create a stable cache key from the SQL string plus params.
    Avoid recompiling with literal binds to keep the key stable across calls.
    """
    sql_text = str(statement)
    raw = f"{sql_text}|{repr(params)}"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"{prefix}:{digest}"


class CachedAsyncSession(AsyncSession):
    """
    AsyncSession subclass that transparently caches SELECT queries in Redis using pickle.
    """

    def __init__(
        self,
        *args,
        redis_client: aioredis.Redis,
        cache_prefix: str = "sqlcache",
        default_ttl: int = 60,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._redis = redis_client
        self._cache_prefix = cache_prefix
        self._ttl = default_ttl
        # Track whether the last execute returned from cache (True), DB (False), or unknown (None)
        self._last_from_cache = None
        # Simple in-memory fallback cache: {key: (payload, expiry_ts)}
        self._local_cache = {}

    async def execute(self, statement: Executable, params: Optional[dict] = None, **kwargs: Any):
        params = params or {}

        # Only cache SELECT statements
        is_select = isinstance(statement, Select)
        if not is_select:
            with contextlib.suppress(Exception):
                txt = str(statement).strip().lower()
                if txt.startswith("select"):
                    is_select = True
        if not is_select:
            # Not a SELECT -> bypass cache
            return await super().execute(statement, params=params, **kwargs)

        # Build cache key
        cache_key = make_cache_key(statement, params, prefix=self._cache_prefix)

        # 1) Try cache (Redis first; then local fallback). Don't fail if cache is down.
        try:
            cached = await self._redis.get(cache_key)
        except Exception:
            cached = None
        if cached:
            print("🔹 Returning from cache")
            self._last_from_cache = True
            return pickle.loads(cached)
        # Local fallback
        entry = self._local_cache.get(cache_key)
        if entry is not None:
            payload, expiry = entry
            if expiry > time.time():
                print("🔹 Returning from cache (local)")
                self._last_from_cache = True
                return payload
            else:
                # expired
                self._local_cache.pop(cache_key, None)

        # 2) Otherwise hit DB
        print("⚡ Hitting the database")
        result = await super().execute(statement, params=params, **kwargs)
        self._last_from_cache = False

        # Materialize a portable payload (list of dicts or primitives)
        payload = []
        try:
            if scalars_list := result.scalars().all():
                first = scalars_list[0]
                if hasattr(first, "__mapper__"):
                    # ORM models -> serialize columns
                    try:
                        payload = [
                            {attr.key: getattr(obj, attr.key) for attr in sa_inspect(obj).mapper.column_attrs}
                            for obj in scalars_list
                        ]
                    except Exception:
                        payload = []
                else:
                    # Primitives (ints, strs, etc.)
                    payload = list(scalars_list)
            else:
                # No scalars; try mappings (e.g., select of specific columns)
                try:
                    rows = result.mappings().all()
                    payload = [dict(r) for r in rows]
                except Exception:
                    payload = []
        except Exception:
            try:
                rows = result.mappings().all()
                payload = [dict(r) for r in rows]
            except Exception:
                payload = []

        # 3) Store in Redis
        with contextlib.suppress(Exception):
            await self._redis.set(cache_key, pickle.dumps(payload), ex=self._ttl)
        # Always set local fallback
        self._local_cache[cache_key] = (payload, time.time() + float(self._ttl))

        return payload

    async def clear_cache(self):
        """Clear all cached queries (prefix-based)."""
        keys = await self._redis.keys(f"{self._cache_prefix}:*")
        if keys:
            await self._redis.delete(*keys)


def register_simple_invalidation(engine, redis_client: aioredis.Redis, cache_prefix: str = "sqlcache"):
    """
    Simple invalidation: clears all cache on insert/update/delete.
    """
    async def _clear_cache():
        keys = await redis_client.keys(f"{cache_prefix}:*")
        if keys:
            await redis_client.delete(*keys)

    @event.listens_for(engine.sync_engine, "after_execute")
    def _after_execute(conn, clauseelement, multiparams, params, result):
        with contextlib.suppress(Exception):
            txt = str(clauseelement).strip().lower()
            if txt.startswith(("insert", "update", "delete")):
                loop = asyncio.get_running_loop()
                loop.create_task(_clear_cache())
