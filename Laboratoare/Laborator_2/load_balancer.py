#!/usr/bin/env python3
"""
Round-Robin load balancer (thread-safe) with deterministic routing by key.
"""
import threading
from typing import List
import hashlib

class LoadBalancer:
    def __init__(self, backends: List[str]):
        if not backends:
            raise ValueError("backends must be non-empty")
        self.backends = list(backends)
        self._idx = 0
        self._lock = threading.Lock()

    def next(self) -> str:
        with self._lock:
            url = self.backends[self._idx]
            self._idx = (self._idx + 1) % len(self.backends)
            return url

    def backend_for_key(self, key: str) -> str:
        if not key:
            return self.next()
        h = hashlib.md5(key.encode('utf-8')).hexdigest()
        idx = int(h, 16) % len(self.backends)
        return self.backends[idx]

    def add(self, backend: str):
        with self._lock:
            self.backends.append(backend)

    def remove(self, backend: str):
        with self._lock:
            self.backends = [b for b in self.backends if b != backend]
            if self._idx >= len(self.backends):
                self._idx = 0