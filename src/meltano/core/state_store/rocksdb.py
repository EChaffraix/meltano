"""A state store manager that uses RocksDB as the backend."""

from __future__ import annotations

import fnmatch
from urllib.parse import urlparse

from rocksdict import Rdict

from meltano.core.job_state import JobState
from meltano.core.state_store.base import StateStoreManager


class RocksDBStateStoreManager(StateStoreManager):
    """A state store manager that uses RocksDB as the backend."""

    label: str = "On-disk RocksDB key-value store"

    def __init__(self, uri: str, **kwargs) -> None:
        """Create a RocksDBStateStoreManager.

        Args:
            uri: The URI to use to connect to the RocksDB database
            **kwargs: Additional kwargs to pass to the underlying rocksdict.Rdict
        """
        super().__init__(**kwargs)
        self.uri = uri
        self.parsed = urlparse(self.uri)
        self.path = self.parsed.path
        self.db = Rdict(self.path)

    def set(self, state: JobState) -> None:
        """Set state for the given state_id.

        Args:
            state: The state to set
        """
        if state.is_complete():
            self.db[state.state_id] = state.to_dict()
            return

        existing_state = self.db.get(state.state_id)
        if existing_state:
            state_to_write = JobState.from_object(state.state_id, existing_state)
            state_to_write.merge_partial(state)
        else:
            state_to_write = state

        self.db[state.state_id] = state_to_write.to_dict()

    def get(self, state_id: str) -> JobState | None:
        """Get state for the given state_id.

        Args:
            state_id: The state_id to get state for

        Returns:
            Dict representing state that would be used in the next run.
        """
        state_dict: dict | None = self.db.get(state_id)
        return JobState.from_object(state_id, state_dict) if state_dict else None

    def clear(self, state_id: str):
        """Clear state for the given state_id.

        Args:
            state_id: the state_id to clear state for
        """
        self.db.delete(state_id)

    def get_state_ids(self, pattern: str | None = None):
        """Get all state_ids available in this state store manager.

        Args:
            pattern: glob-style pattern to filter by

        Returns:
            List of state_ids available in this state store manager.
        """
        return [
            state_id
            for state_id in self.db.keys()  # noqa: SIM118
            if not pattern or fnmatch.fnmatch(state_id, pattern)  # type: ignore
        ]

    def acquire_lock(self, state_id: str):
        """Acquire a naive lock for the given job's state.

        Args:
            state_id: the state_id to lock
        """
        pass
