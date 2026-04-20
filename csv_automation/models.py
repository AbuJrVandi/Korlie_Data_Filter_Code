from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Literal

DateFilterMode = Literal["single", "range"]


@dataclass(frozen=True)
class DateFilterSpec:
    mode: DateFilterMode
    single_date: date | None = None
    start_date: date | None = None
    end_date: date | None = None


@dataclass(frozen=True)
class ColumnRule:
    column: str
    value: str


@dataclass(frozen=True)
class DatasetFilterSpec:
    dataset_name: str
    rules: list[ColumnRule] = field(default_factory=list)


@dataclass(frozen=True)
class DatasetResult:
    dataset_name: str
    input_rows: int
    output_rows: int
    invalid_date_rows: int
    output_filename: str
    csv_bytes: bytes


@dataclass(frozen=True)
class CleaningSpec:
    trim_whitespace: bool = False
    drop_fully_empty_rows: bool = False
    drop_duplicate_rows: bool = False
    required_columns: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class GenericColumnProfile:
    column_name: str
    inferred_dtype: str
    missing_count: int
    missing_percent: float
    unique_count: int
    sample_value: str
    is_date_candidate: bool = False


@dataclass(frozen=True)
class GenericDatasetProfile:
    row_count: int
    column_count: int
    duplicate_rows: int
    fully_empty_rows: int
    date_candidates: list[str] = field(default_factory=list)
    columns: list[GenericColumnProfile] = field(default_factory=list)


@dataclass(frozen=True)
class GenericDatasetResult:
    dataset_name: str
    input_rows: int
    rows_after_cleaning: int
    output_rows: int
    invalid_date_rows: int
    removed_empty_rows: int
    removed_duplicate_rows: int
    removed_missing_required_rows: int
    output_filename: str
    csv_bytes: bytes
