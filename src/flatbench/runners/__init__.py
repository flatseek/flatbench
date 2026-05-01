"""Runner interface and registry for benchmark engines."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class BenchmarkResult:
    """Single benchmark result."""
    engine: str
    dataset: str
    operation: str
    rows: int
    duration_ms: float
    ops_per_sec: float
    latency_p50_ms: float = 0.0
    latency_p95_ms: float = 0.0
    latency_p99_ms: float = 0.0
    memory_mb: float = 0.0
    error: str = ""
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "engine": self.engine,
            "dataset": self.dataset,
            "operation": self.operation,
            "rows": self.rows,
            "duration_ms": round(self.duration_ms, 3),
            "ops_per_sec": round(self.ops_per_sec, 2),
            "latency_p50_ms": round(self.latency_p50_ms, 3),
            "latency_p95_ms": round(self.latency_p95_ms, 3),
            "latency_p99_ms": round(self.latency_p99_ms, 3),
            "memory_mb": round(self.memory_mb, 2),
            "error": self.error,
            **self.metadata
        }


@dataclass
class EngineConfig:
    """Configuration for an engine."""
    name: str
    data_dir: str
    index_path: str = ""
    connection_string: str = ""
    options: dict = field(default_factory=dict)


class BaseRunner(ABC):
    """Base class for all benchmark runners."""

    name: str = "base"
    supports_aggregate: bool = True
    supports_range_query: bool = True
    supports_wildcard: bool = True

    def __init__(self, config: EngineConfig):
        self.config = config

    @abstractmethod
    def build_index(self, data_path: str, workers: int = 1) -> BenchmarkResult:
        """Build index from data file."""
        pass

    @abstractmethod
    def search(self, query: str, **kwargs) -> BenchmarkResult:
        """Run a search query."""
        pass

    @abstractmethod
    def aggregate(self, field: str, **kwargs) -> BenchmarkResult:
        """Run an aggregation query."""
        pass

    def cleanup(self):
        """Clean up resources."""
        pass


# Registry of available engines
_ENGINES: dict[str, type[BaseRunner]] = {}

def register_engine(name: str):
    """Decorator to register an engine runner."""
    def decorator(cls: type[BaseRunner]):
        _ENGINES[name] = cls
        cls.name = name
        return cls
    return decorator

def get_engine(name: str) -> type[BaseRunner]:
    """Get engine runner class by name."""
    if name not in _ENGINES:
        raise ValueError(f"Unknown engine: {name}. Available: {list(_ENGINES.keys())}")
    return _ENGINES[name]

def list_engines() -> list[str]:
    """List all available engine names."""
    return list(_ENGINES.keys())