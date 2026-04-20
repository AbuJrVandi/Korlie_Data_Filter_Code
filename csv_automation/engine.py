from __future__ import annotations

from io import BytesIO
from os.path import splitext

import pandas as pd

from models import (
    CleaningSpec,
    ColumnRule,
    DatasetFilterSpec,
    DatasetResult,
    DateFilterSpec,
    GenericColumnProfile,
    GenericDatasetProfile,
    GenericDatasetResult,
)

REQUIRED_DATE_COLUMN = "created_at"
TENKIPAY_DATASET_NAME = "TenkiPay"
TENKIPAY_TRANSACTION_TYPE_COLUMN = "paidto"
TENKIPAY_TRANSACTION_TYPE_VALUE = "WANGOV"
DATE_COLUMN_HINTS = (
    "date",
    "time",
    "timestamp",
    "created",
    "updated",
    "posted",
    "paid",
    "transaction",
)


def get_csv_columns(file_bytes: bytes) -> tuple[list[str], str | None]:
    try:
        df = pd.read_csv(BytesIO(file_bytes), nrows=0)
    except Exception as exc:  # pragma: no cover - pandas error details vary
        return [], f"Could not read CSV headers: {exc}"

    return list(df.columns), None


def load_csv_dataframe(file_bytes: bytes) -> tuple[pd.DataFrame | None, str | None]:
    if not file_bytes:
        return None, "Uploaded file is empty."

    try:
        df = pd.read_csv(BytesIO(file_bytes))
    except Exception as exc:  # pragma: no cover - pandas error details vary
        return None, f"Could not read CSV file. {exc}"

    return df, None


def process_datasets(
    uploaded_files: dict[str, tuple[str, bytes]],
    date_filter: DateFilterSpec,
    dataset_filters: list[DatasetFilterSpec],
) -> tuple[list[DatasetResult], list[str]]:
    errors = _validate_date_filter(date_filter)
    if errors:
        return [], errors

    filter_map = {spec.dataset_name: spec for spec in dataset_filters}
    dataframes: dict[str, tuple[str, pd.DataFrame]] = {}
    validation_errors: list[str] = []

    for dataset_name, upload in uploaded_files.items():
        filename, file_bytes = upload
        df, dataset_errors = _load_and_validate_dataset(
            dataset_name=dataset_name,
            filename=filename,
            file_bytes=file_bytes,
            dataset_filter=filter_map.get(dataset_name, DatasetFilterSpec(dataset_name=dataset_name)),
        )

        if dataset_errors:
            validation_errors.extend(dataset_errors)
            continue

        if df is not None:
            dataframes[dataset_name] = (filename, df)

    if validation_errors:
        return [], validation_errors

    results: list[DatasetResult] = []
    for dataset_name, (filename, df) in dataframes.items():
        dataset_filter = filter_map.get(dataset_name, DatasetFilterSpec(dataset_name=dataset_name))
        result = _filter_dataset(
            dataset_name=dataset_name,
            original_filename=filename,
            df=df,
            date_filter=date_filter,
            rules=dataset_filter.rules,
            date_column=REQUIRED_DATE_COLUMN,
        )
        results.append(result)

    return results, []


def build_generic_profile(df: pd.DataFrame) -> GenericDatasetProfile:
    date_candidates = detect_date_columns(df)
    date_candidate_set = set(date_candidates)
    empty_rows = _fully_empty_row_mask(df)
    column_profiles: list[GenericColumnProfile] = []

    for column_name in df.columns:
        series = df[column_name]
        non_null = series.dropna()
        sample_value = ""
        if not non_null.empty:
            sample_value = str(non_null.iloc[0])[:80]

        column_profiles.append(
            GenericColumnProfile(
                column_name=column_name,
                inferred_dtype=str(series.dtype),
                missing_count=int(series.isna().sum()),
                missing_percent=round(float(series.isna().mean() * 100), 2),
                unique_count=int(non_null.nunique(dropna=True)),
                sample_value=sample_value,
                is_date_candidate=column_name in date_candidate_set,
            )
        )

    return GenericDatasetProfile(
        row_count=len(df),
        column_count=len(df.columns),
        duplicate_rows=int(df.duplicated().sum()),
        fully_empty_rows=int(empty_rows.sum()),
        date_candidates=date_candidates,
        columns=column_profiles,
    )


def detect_date_columns(df: pd.DataFrame) -> list[str]:
    scored_columns: list[tuple[float, str]] = []

    for column_name in df.columns:
        series = df[column_name].dropna()
        if series.empty:
            continue

        sample = series.astype(str).head(500)
        parsed_dates = _parse_datetime_series(sample)
        parse_ratio = float(parsed_dates.notna().mean())
        name_lower = column_name.lower()
        has_hint = any(hint in name_lower for hint in DATE_COLUMN_HINTS)
        threshold = 0.35 if has_hint else 0.8

        if parse_ratio >= threshold:
            score = parse_ratio + (0.1 if has_hint else 0.0)
            scored_columns.append((score, column_name))

    scored_columns.sort(key=lambda item: (-item[0], item[1].lower()))
    return [column_name for _, column_name in scored_columns]


def process_generic_dataset(
    dataset_name: str,
    original_filename: str,
    file_bytes: bytes,
    cleaning_spec: CleaningSpec,
    rules: list[ColumnRule],
    apply_date_filter: bool = False,
    date_column: str | None = None,
    date_filter: DateFilterSpec | None = None,
) -> tuple[GenericDatasetResult | None, list[str]]:
    df, error = load_csv_dataframe(file_bytes)
    if error:
        return None, [error]
    if df is None:
        return None, ["Could not load dataset."]

    validation_errors = _validate_generic_request(
        df=df,
        cleaning_spec=cleaning_spec,
        rules=rules,
        apply_date_filter=apply_date_filter,
        date_column=date_column,
        date_filter=date_filter,
    )
    if validation_errors:
        return None, validation_errors

    cleaned_df, cleaning_metrics = _apply_cleaning(df, cleaning_spec)

    working_df = cleaned_df.copy()
    invalid_date_rows = 0

    if apply_date_filter and date_filter is not None and date_column is not None:
        parsed_dates = _parse_datetime_series(working_df[date_column])
        invalid_date_rows = int(parsed_dates.isna().sum())
        working_df = working_df.loc[_build_date_mask(parsed_dates, date_filter)].copy()

    for rule in rules:
        working_df = working_df.loc[working_df[rule.column].astype(str) == rule.value].copy()

    csv_bytes = working_df.to_csv(index=False).encode("utf-8")

    return (
        GenericDatasetResult(
            dataset_name=dataset_name,
            input_rows=len(df),
            rows_after_cleaning=len(cleaned_df),
            output_rows=len(working_df),
            invalid_date_rows=invalid_date_rows,
            removed_empty_rows=cleaning_metrics["removed_empty_rows"],
            removed_duplicate_rows=cleaning_metrics["removed_duplicate_rows"],
            removed_missing_required_rows=cleaning_metrics["removed_missing_required_rows"],
            output_filename=_build_output_filename(original_filename),
            csv_bytes=csv_bytes,
        ),
        [],
    )


def _load_and_validate_dataset(
    dataset_name: str,
    filename: str,
    file_bytes: bytes,
    dataset_filter: DatasetFilterSpec,
) -> tuple[pd.DataFrame | None, list[str]]:
    errors: list[str] = []
    df, error = load_csv_dataframe(file_bytes)
    if error:
        errors.append(f"{dataset_name}: {error}")
        return None, errors
    if df is None:
        errors.append(f"{dataset_name}: uploaded file could not be loaded.")
        return None, errors

    if REQUIRED_DATE_COLUMN not in df.columns:
        errors.append(
            f"{dataset_name}: missing required '{REQUIRED_DATE_COLUMN}' column in '{filename}'."
        )

    if dataset_name == TENKIPAY_DATASET_NAME and TENKIPAY_TRANSACTION_TYPE_COLUMN not in df.columns:
        errors.append(
            f"{dataset_name}: missing required '{TENKIPAY_TRANSACTION_TYPE_COLUMN}' column in '{filename}'."
        )

    for rule in dataset_filter.rules:
        if rule.column not in df.columns:
            errors.append(
                f"{dataset_name}: filter column '{rule.column}' does not exist in '{filename}'."
            )

    if errors:
        return None, errors

    return df, []


def _filter_dataset(
    dataset_name: str,
    original_filename: str,
    df: pd.DataFrame,
    date_filter: DateFilterSpec,
    rules: list[ColumnRule],
    date_column: str,
) -> DatasetResult:
    parsed_dates = _parse_datetime_series(df[date_column])
    invalid_date_rows = int(parsed_dates.isna().sum())
    filtered_df = df.loc[_build_date_mask(parsed_dates, date_filter)].copy()

    if dataset_name == TENKIPAY_DATASET_NAME:
        filtered_df = filtered_df.loc[
            filtered_df[TENKIPAY_TRANSACTION_TYPE_COLUMN].astype(str) == TENKIPAY_TRANSACTION_TYPE_VALUE
        ].copy()

    for rule in rules:
        filtered_df = filtered_df.loc[filtered_df[rule.column].astype(str) == rule.value].copy()

    csv_bytes = filtered_df.to_csv(index=False).encode("utf-8")

    return DatasetResult(
        dataset_name=dataset_name,
        input_rows=len(df),
        output_rows=len(filtered_df),
        invalid_date_rows=invalid_date_rows,
        output_filename=_build_output_filename(original_filename),
        csv_bytes=csv_bytes,
    )


def _validate_generic_request(
    df: pd.DataFrame,
    cleaning_spec: CleaningSpec,
    rules: list[ColumnRule],
    apply_date_filter: bool,
    date_column: str | None,
    date_filter: DateFilterSpec | None,
) -> list[str]:
    errors: list[str] = []

    for required_column in cleaning_spec.required_columns:
        if required_column not in df.columns:
            errors.append(f"Required-value cleaning column '{required_column}' does not exist.")

    for rule in rules:
        if rule.column not in df.columns:
            errors.append(f"Filter column '{rule.column}' does not exist in the uploaded dataset.")

    if not apply_date_filter:
        return errors

    if not date_column:
        errors.append("Choose a date column before applying date filtering.")
        return errors

    if date_column not in df.columns:
        errors.append(f"Selected date column '{date_column}' does not exist in the uploaded dataset.")

    if date_filter is None:
        errors.append("Choose a valid single date or date range before processing.")
    else:
        errors.extend(_validate_date_filter(date_filter))

    return errors


def _apply_cleaning(df: pd.DataFrame, cleaning_spec: CleaningSpec) -> tuple[pd.DataFrame, dict[str, int]]:
    working_df = df.copy()

    if cleaning_spec.trim_whitespace:
        object_columns = working_df.select_dtypes(include=["object", "string"]).columns
        for column_name in object_columns:
            working_df[column_name] = working_df[column_name].map(
                lambda value: value.strip() if isinstance(value, str) else value
            )

    removed_empty_rows = 0
    if cleaning_spec.drop_fully_empty_rows:
        empty_mask = _fully_empty_row_mask(working_df)
        removed_empty_rows = int(empty_mask.sum())
        working_df = working_df.loc[~empty_mask].copy()

    removed_missing_required_rows = 0
    if cleaning_spec.required_columns:
        missing_required_mask = _missing_required_mask(working_df, cleaning_spec.required_columns)
        removed_missing_required_rows = int(missing_required_mask.sum())
        working_df = working_df.loc[~missing_required_mask].copy()

    removed_duplicate_rows = 0
    if cleaning_spec.drop_duplicate_rows:
        duplicate_mask = working_df.duplicated()
        removed_duplicate_rows = int(duplicate_mask.sum())
        working_df = working_df.loc[~duplicate_mask].copy()

    return (
        working_df,
        {
            "removed_empty_rows": removed_empty_rows,
            "removed_duplicate_rows": removed_duplicate_rows,
            "removed_missing_required_rows": removed_missing_required_rows,
        },
    )


def _fully_empty_row_mask(df: pd.DataFrame) -> pd.Series:
    normalized_df = _normalize_blank_strings(df)
    return normalized_df.isna().all(axis=1)


def _missing_required_mask(df: pd.DataFrame, required_columns: list[str]) -> pd.Series:
    normalized_df = _normalize_blank_strings(df[required_columns])
    return normalized_df.isna().any(axis=1)


def _normalize_blank_strings(df: pd.DataFrame) -> pd.DataFrame:
    return df.replace(r"^\s*$", pd.NA, regex=True)


def _parse_datetime_series(series: pd.Series) -> pd.Series:
    try:
        return pd.to_datetime(series, format="mixed", errors="coerce")
    except TypeError:
        return pd.to_datetime(series, errors="coerce")


def _build_date_mask(parsed_dates: pd.Series, date_filter: DateFilterSpec) -> pd.Series:
    normalized_dates = parsed_dates.dt.date

    if date_filter.mode == "single":
        return normalized_dates == date_filter.single_date

    return normalized_dates.between(date_filter.start_date, date_filter.end_date)


def _validate_date_filter(date_filter: DateFilterSpec) -> list[str]:
    if date_filter.mode == "single":
        if date_filter.single_date is None:
            return ["Single-date mode requires a date."]
        return []

    if date_filter.mode == "range":
        if date_filter.start_date is None or date_filter.end_date is None:
            return ["Date-range mode requires both a start date and an end date."]
        if date_filter.end_date < date_filter.start_date:
            return ["Date-range mode requires the end date to be on or after the start date."]
        return []

    return [f"Unsupported date filter mode: {date_filter.mode}"]


def _build_output_filename(original_filename: str) -> str:
    stem, ext = splitext(original_filename)
    extension = ext or ".csv"
    return f"{stem}__filtered{extension}"
