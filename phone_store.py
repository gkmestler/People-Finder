"""In-memory store for async phone reveal webhook results."""

from __future__ import annotations

import threading
import time
import uuid


class PhoneRevealJob:
    """Tracks a batch of phone reveal requests and collects webhook responses."""

    def __init__(self, person_ids: list[str], timeout: float = 60.0):
        self.job_id = uuid.uuid4().hex[:12]
        self.person_ids = set(person_ids)
        self.expected = len(self.person_ids)
        self.results: dict[str, dict] = {}  # person_id -> phone data
        self._lock = threading.Lock()
        self._event = threading.Event()
        self.timeout = timeout
        self.created_at = time.time()

    def record_phone(self, person_id: str, phone_data: dict):
        """Record phone data received via webhook."""
        with self._lock:
            self.results[person_id] = phone_data
            if len(self.results) >= self.expected:
                self._event.set()

    def wait(self) -> dict[str, dict]:
        """Block until all results arrive or timeout. Returns person_id -> phone_data."""
        self._event.wait(timeout=self.timeout)
        with self._lock:
            return dict(self.results)


class PhoneRevealStore:
    """Singleton store for phone reveal jobs."""

    def __init__(self):
        self._jobs: dict[str, PhoneRevealJob] = {}
        self._lock = threading.Lock()

    def create_job(self, person_ids: list[str], timeout: float = 60.0) -> PhoneRevealJob:
        job = PhoneRevealJob(person_ids, timeout)
        with self._lock:
            self._jobs[job.job_id] = job
            self._cleanup_old()
        return job

    def get_job(self, job_id: str) -> PhoneRevealJob | None:
        with self._lock:
            return self._jobs.get(job_id)

    def remove_job(self, job_id: str):
        with self._lock:
            self._jobs.pop(job_id, None)

    def _cleanup_old(self):
        """Remove jobs older than 5 minutes."""
        cutoff = time.time() - 300
        stale = [jid for jid, j in self._jobs.items() if j.created_at < cutoff]
        for jid in stale:
            del self._jobs[jid]


phone_store = PhoneRevealStore()
