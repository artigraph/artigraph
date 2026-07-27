from __future__ import annotations

from arti.backends.memory import MemoryBackend


def test_memory_backend_context_manager() -> None:
    with MemoryBackend() as backend:
        assert isinstance(backend, MemoryBackend)
    # Closing a MemoryBackend is a no-op.
    with backend.connect():
        pass
