from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Type
from app.filters.base import BaseFilter
from app.filters.fetch_raw import FetchRawFilter
from app.filters.unzip_package import UnzipPackageFilter
from app.filters.analyze import AnalyzeFilter
from app.filters.summarize import SummarizeResultFilter

@dataclass
class RegistryItem:
    step_type: str; filter_name: str; filter_version: str; filter_class: Type[BaseFilter]
    input_artifact_type: str | None; output_artifact_type: str; output_path_builder: Callable

class FilterRegistry:
    def __init__(self): self._items={}
    def register(self, item: RegistryItem): self._items[item.step_type]=item
    def get(self, step_type): return self._items[step_type]
    def step_types(self): return list(self._items.keys())

registry=FilterRegistry()
registry.register(RegistryItem("fetch_raw","FetchRawFilter","1.1.0",FetchRawFilter,None,"raw_package",lambda root, req, key: root/"artifacts"/"raw_packages"/req["source"]/req["dataset"]/key))
registry.register(RegistryItem("unzip_package","UnzipPackageFilter","1.0.0",UnzipPackageFilter,"raw_package","raw",lambda root, req, key: root/"artifacts"/"raw"/req["source"]/req["dataset"]/key))
registry.register(RegistryItem("analyze_phrase_stats","AnalyzeFilter","1.0.0",AnalyzeFilter,"raw","result",lambda root, req, key: root/"artifacts"/"results"/"phrase_stats"/key))
registry.register(RegistryItem("summarize_result","SummarizeResultFilter","1.0.0",SummarizeResultFilter,"result","summary",lambda root, req, key: root/"artifacts"/"results"/"summary"/key))
