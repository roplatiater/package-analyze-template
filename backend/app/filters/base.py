from abc import ABC, abstractmethod
from typing import Any
from dataclasses import dataclass
from pathlib import Path

@dataclass
class FilterOutput:
    files: list[Path]

class BaseFilter(ABC):
    name: str
    version: str
    @abstractmethod
    def build_cache_key(self, context: dict[str, Any]) -> str: ...
    @abstractmethod
    async def run(self, context: dict[str, Any], tmp_dir: Path) -> FilterOutput: ...
    @abstractmethod
    def build_manifest(self, context: dict[str, Any], output_files: FilterOutput) -> dict[str, Any]: ...
