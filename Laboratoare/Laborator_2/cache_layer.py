#!/usr/bin/env python3
"""
Cache layer with Redis (if available) else in-memory fallback with TTL.
"""
import time
import threading
from typing import Optional
import logging

logger = logging.getLogger("cache_layer")

try:
    import redis
except Exception:
    redis = None

class CacheLayer:
    def __init__(self, host='localhost', port=6379):
        self._use_redis = False
        self._redis = None
        if redis is not None:
            try:
                self._redis = redis.Redis(host=host, port=port, decode_responses=True)
                self._redis.ping()
                self._use_redis = True
                logger.info('CacheLayer: using Redis at %s:%d', host, port)
            except Exception:
                logger.info('CacheLayer: Redis not available, using in-memory fallback')

        self._store = {}
        self._lock = threading.RLock()
        self._stop = False
        self._cleaner = threading.Thread(target=self._evict_loop, daemon=True)
        self._cleaner.start()

    def _evict_loop(self):
        while not self._stop:
            now = time.time()
            with self._lock:
                keys = list(self._store.keys())
                for k in keys:
                    _, exp = self._store.get(k, (None, 0))
                    if exp and exp <= now:
                        self._store.pop(k, None)
            time.sleep(1)

    def put(self, key: str, value: str, ttl_seconds: int = 30):
        if self._use_redis:
            try:
                self._redis.setex(key, ttl_seconds, value)
                return
            except Exception:
                self._use_redis = False
        with self._lock:
            expiry = time.time() + ttl_seconds if ttl_seconds else 0
            self._store[key] = (value, expiry)

    def get(self, key: str) -> Optional[str]:
        if self._use_redis:
            try:
                return self._redis.get(key)
            except Exception:
                self._use_redis = False
        with self._lock:
            entry = self._store.get(key)
            if not entry:
                return None
            value, expiry = entry
            if expiry and expiry <= time.time():
                self._store.pop(key, None)
                return None
            return value

    def invalidate(self, key: str):
        if self._use_redis:
            try:
                self._redis.delete(key)
                return
            except Exception:
                self._use_redis = False
        with self._lock:
            self._store.pop(key, None)

    def stop(self):
        self._stop = True
        self._cleaner.join(timeout=1)