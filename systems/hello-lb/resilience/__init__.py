"""Shared resilience wrapper (circuit breaker + retry) for the hello-lb system.

One implementation imported by every wired service; per-connection behavior is driven
entirely by the manifest `resilience` policy read at runtime. See engine.py.
"""
from .engine import CircuitOpenError, ResilienceRegistry

__all__ = ["ResilienceRegistry", "CircuitOpenError"]
