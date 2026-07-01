from typing import Any, Literal
from pydantic import BaseModel, ConfigDict, Field, field_validator


class JobParams(BaseModel):
    model_config = ConfigDict(extra="allow")

    simulate_fail_stage: Literal["fetch", "analyze", "commit"] | None = None
    delay_ms: int | None = Field(default=None, ge=0, le=60_000)
    force_refresh: bool | None = None
    batch_id: str | None = None
    window_start: str | None = None
    window_end: str | None = None
    phrases: list[str] | None = None
    categories: list[str] | None = None
    value_multiplier: float | None = None
    records: list[dict[str, Any]] | None = None

    @field_validator("phrases", "categories")
    @classmethod
    def non_empty_string_list(cls, value: list[str] | None):
        if value is not None and (not value or any(not item for item in value)):
            raise ValueError("must contain at least one non-empty string")
        return value

    @field_validator("records")
    @classmethod
    def non_empty_records(cls, value: list[dict[str, Any]] | None):
        if value is not None and not value:
            raise ValueError("records must not be empty")
        return value


class CreateJobRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    source: str | None = None
    dataset: str | None = None
    analysis_type: str | None = None
    trigger: str | None = None
    requested_at: str | None = None
    params: JobParams = Field(default_factory=JobParams)
