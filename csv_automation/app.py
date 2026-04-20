from __future__ import annotations

import base64
from pathlib import Path

import pandas as pd
import streamlit as st

from engine import build_generic_profile, get_csv_columns, load_csv_dataframe, process_datasets, process_generic_dataset
from models import CleaningSpec, ColumnRule, DatasetFilterSpec, DateFilterSpec, GenericDatasetProfile
from zip_utils import build_results_zip

DATASET_ORDER = ["TenkiPay", "SLRSA", "GovPay"]
LOGO_PATH = Path("/Users/user/Desktop/CMS_Korlie/public/assets/logo/Korlie_Lion_White_Clear.png")


st.set_page_config(page_title="Korlie Data Automation", layout="wide")


@st.cache_data(show_spinner=False)
def load_logo_base64(path: str) -> str:
    return base64.b64encode(Path(path).read_bytes()).decode("utf-8")


@st.cache_data(show_spinner=False)
def cached_columns(file_name: str, file_bytes: bytes) -> tuple[list[str], str | None]:
    return get_csv_columns(file_bytes)


@st.cache_data(show_spinner=False)
def cached_generic_dataset(file_name: str, file_bytes: bytes) -> tuple[pd.DataFrame | None, GenericDatasetProfile | None, str | None]:
    df, error = load_csv_dataframe(file_bytes)
    if error or df is None:
        return None, None, error or "Could not load the uploaded CSV."

    return df, build_generic_profile(df), None


def inject_styles() -> None:
    st.markdown(
        """
        <style>
            :root {
                --korlie-bg: #f4f7fb;
                --korlie-surface: #ffffff;
                --korlie-surface-soft: #f9fbfd;
                --korlie-border: #d9e2ec;
                --korlie-border-strong: #c6d2df;
                --korlie-text: #162033;
                --korlie-muted: #5f6f84;
                --korlie-accent: #1f4e79;
                --korlie-accent-strong: #173b5c;
                --korlie-shadow: 0 18px 40px rgba(20, 32, 51, 0.08);
            }

            .stApp {
                background:
                    radial-gradient(circle at top left, rgba(31, 78, 121, 0.08), transparent 28%),
                    linear-gradient(180deg, #f8fbff 0%, var(--korlie-bg) 52%, #eef3f8 100%);
                color: var(--korlie-text);
            }

            .main .block-container {
                max-width: 1180px;
                padding-top: 2rem;
                padding-bottom: 4rem;
            }

            h1, h2, h3 {
                color: var(--korlie-text);
                font-family: "Avenir Next", "Segoe UI", sans-serif;
                letter-spacing: 0.02em;
            }

            p, label, .stCaption, .stMarkdown, .stText, .stAlert {
                color: var(--korlie-text);
            }

            [data-testid="stMarkdownContainer"] p {
                color: var(--korlie-text);
            }

            .stTabs [data-baseweb="tab-list"] {
                gap: 0.75rem;
                margin-bottom: 0.75rem;
            }

            .stTabs [data-baseweb="tab"] {
                height: auto;
                padding: 0.8rem 1rem;
                background: rgba(255, 255, 255, 0.84);
                border: 1px solid var(--korlie-border);
                border-radius: 14px;
                color: var(--korlie-text);
                box-shadow: var(--korlie-shadow);
            }

            .stTabs [aria-selected="true"] {
                background: linear-gradient(180deg, var(--korlie-accent) 0%, var(--korlie-accent-strong) 100%);
                color: #ffffff;
                border-color: var(--korlie-accent-strong);
            }

            [data-testid="stSidebar"] {
                background: linear-gradient(180deg, #173551 0%, #1f4e79 100%);
                border-right: 1px solid rgba(255, 255, 255, 0.08);
            }

            [data-testid="stSidebar"] * {
                color: #f7fbff;
            }

            [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {
                color: #f7fbff;
            }

            [data-testid="stSidebar"] [data-testid="stRadio"] {
                background: rgba(255, 255, 255, 0.08);
                border: 1px solid rgba(255, 255, 255, 0.14);
                box-shadow: none;
            }

            [data-testid="stSidebar"] [data-testid="stRadio"] label {
                color: #f7fbff;
            }

            [data-testid="stSidebar"] [data-testid="stRadio"] > div {
                gap: 0.65rem;
            }

            .korlie-sidebar-card {
                padding: 1rem 1rem 1.1rem;
                margin-bottom: 1rem;
                border-radius: 18px;
                background: linear-gradient(180deg, rgba(255, 255, 255, 0.12) 0%, rgba(255, 255, 255, 0.06) 100%);
                border: 1px solid rgba(255, 255, 255, 0.14);
                box-shadow: 0 18px 32px rgba(11, 24, 39, 0.18);
            }

            .korlie-sidebar-eyebrow {
                margin: 0 0 0.35rem;
                font-size: 0.75rem;
                letter-spacing: 0.18em;
                text-transform: uppercase;
                color: rgba(247, 251, 255, 0.72);
                font-weight: 700;
            }

            .korlie-sidebar-title {
                margin: 0;
                font-size: 1.15rem;
                font-weight: 700;
                color: #ffffff;
            }

            .korlie-sidebar-copy {
                margin: 0.6rem 0 0;
                font-size: 0.93rem;
                line-height: 1.55;
                color: rgba(247, 251, 255, 0.82);
            }

            .korlie-sidebar-label {
                margin: 1.1rem 0 0.45rem;
                font-size: 0.74rem;
                letter-spacing: 0.18em;
                text-transform: uppercase;
                color: rgba(247, 251, 255, 0.68);
                font-weight: 700;
            }

            [data-testid="stFileUploader"],
            [data-testid="stNumberInput"],
            [data-testid="stDateInput"],
            [data-testid="stSelectbox"],
            [data-testid="stTextInput"],
            [data-testid="stRadio"],
            [data-testid="stMultiSelect"],
            [data-testid="stDataFrame"],
            [data-testid="stMetric"] {
                background: var(--korlie-surface);
                border: 1px solid var(--korlie-border);
                border-radius: 16px;
                padding: 0.75rem 0.9rem;
                box-shadow: var(--korlie-shadow);
            }

            [data-testid="stFileUploaderDropzone"] {
                background: var(--korlie-surface-soft);
                border: 1.5px dashed var(--korlie-border-strong);
                border-radius: 14px;
            }

            [data-testid="stFileUploaderDropzone"] * {
                color: var(--korlie-text);
            }

            [data-testid="stFileUploaderDropzone"] button {
                background: #f2f6fb;
                border: 1px solid var(--korlie-border);
                color: var(--korlie-accent);
            }

            [data-testid="stFileUploaderDropzone"]:hover {
                border-color: var(--korlie-accent);
                background: #f6f9fd;
            }

            [data-baseweb="input"] > div,
            [data-baseweb="select"] > div,
            [data-baseweb="tag"] {
                background: var(--korlie-surface-soft);
                border-color: var(--korlie-border);
                color: var(--korlie-text);
            }

            .stRadio > div {
                gap: 0.9rem;
            }

            .stRadio label {
                color: var(--korlie-text);
            }

            div[data-testid="stButton"] > button,
            div[data-testid="stDownloadButton"] > button {
                border-radius: 12px;
                border: 1px solid var(--korlie-accent);
                background: linear-gradient(180deg, var(--korlie-accent) 0%, var(--korlie-accent-strong) 100%);
                color: #ffffff;
                font-weight: 600;
                letter-spacing: 0.02em;
                min-height: 3rem;
                box-shadow: 0 12px 24px rgba(31, 78, 121, 0.16);
            }

            div[data-testid="stButton"] > button:hover,
            div[data-testid="stDownloadButton"] > button:hover {
                border-color: var(--korlie-accent-strong);
                background: var(--korlie-accent-strong);
                color: #ffffff;
            }

            .korlie-hero {
                position: relative;
                overflow: hidden;
                display: flex;
                align-items: center;
                gap: 1.5rem;
                padding: 1.75rem 1.9rem;
                margin-bottom: 1.2rem;
                border: 1px solid var(--korlie-border);
                border-radius: 28px;
                background:
                    radial-gradient(circle at top right, rgba(31, 78, 121, 0.08), transparent 30%),
                    linear-gradient(180deg, #ffffff 0%, #f7fafe 100%);
                box-shadow: 0 24px 54px rgba(20, 32, 51, 0.08);
            }

            .korlie-hero::after {
                content: "";
                position: absolute;
                inset: auto -8% -40% auto;
                width: 260px;
                height: 260px;
                border-radius: 50%;
                background: radial-gradient(circle, rgba(31, 78, 121, 0.10), transparent 65%);
            }

            .korlie-logo-wrap {
                width: 96px;
                height: 96px;
                flex: 0 0 96px;
                display: grid;
                place-items: center;
                border-radius: 24px;
                background: linear-gradient(180deg, #264f78 0%, #173551 100%);
                border: 1px solid rgba(23, 59, 92, 0.28);
                box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.12);
            }

            .korlie-logo-wrap img {
                width: 74px;
                height: auto;
                display: block;
            }

            .korlie-eyebrow {
                margin: 0 0 0.35rem;
                font-size: 0.78rem;
                letter-spacing: 0.26em;
                text-transform: uppercase;
                color: var(--korlie-accent);
                font-weight: 700;
            }

            .korlie-title {
                margin: 0;
                font-size: clamp(2rem, 4vw, 3.2rem);
                line-height: 1;
                font-weight: 750;
            }

            .korlie-subtitle {
                margin: 0.7rem 0 0;
                max-width: 760px;
                color: var(--korlie-muted);
                font-size: 1rem;
                line-height: 1.6;
            }

            .korlie-section-label {
                margin: 1.6rem 0 0.65rem;
                color: var(--korlie-accent);
                font-size: 0.86rem;
                font-weight: 700;
                letter-spacing: 0.16em;
                text-transform: uppercase;
            }

            .korlie-note {
                padding: 1rem 1.1rem;
                border: 1px solid var(--korlie-border);
                border-radius: 18px;
                background: linear-gradient(180deg, #ffffff 0%, #f8fbff 100%);
                color: var(--korlie-muted);
                box-shadow: var(--korlie-shadow);
                margin-bottom: 1rem;
            }

            .korlie-footer {
                margin-top: 2rem;
                padding-top: 1.1rem;
                border-top: 1px solid var(--korlie-border);
                text-align: center;
                color: var(--korlie-muted);
                font-size: 0.92rem;
                letter-spacing: 0.06em;
            }

            @media (max-width: 780px) {
                .korlie-hero {
                    flex-direction: column;
                    align-items: flex-start;
                }

                .korlie-logo-wrap {
                    width: 82px;
                    height: 82px;
                    flex-basis: 82px;
                }

                .korlie-logo-wrap img {
                    width: 62px;
                }
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_hero() -> None:
    logo_base64 = load_logo_base64(str(LOGO_PATH))
    st.markdown(
        f"""
        <section class="korlie-hero">
            <div class="korlie-logo-wrap">
                <img src="data:image/png;base64,{logo_base64}" alt="Korlie logo" />
            </div>
            <div>
                <p class="korlie-eyebrow">Korlie Limited</p>
                <h1 class="korlie-title">Korlie Data Automation</h1>
                <p class="korlie-subtitle">
                    Run the fixed Korlie pipeline for TenkiPay, SLRSA, and GovPay datasets,
                    or switch to the custom dataset studio to profile, clean, filter, and export any CSV.
                </p>
            </div>
        </section>
        """,
        unsafe_allow_html=True,
    )


def render_section_label(label: str) -> None:
    st.markdown(f'<p class="korlie-section-label">{label}</p>', unsafe_allow_html=True)


def render_note(text: str) -> None:
    st.markdown(f'<div class="korlie-note">{text}</div>', unsafe_allow_html=True)


def render_footer() -> None:
    st.markdown(
        """
        <div class="korlie-footer">
            2026 Korlie Limited
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar() -> str:
    with st.sidebar:
        st.markdown(
            """
            
            <p class="korlie-sidebar-label">Workflow Navigation</p>
            """,
            unsafe_allow_html=True,
        )
        workflow = st.radio(
            "Workflow Navigation",
            options=["Standard Dataset Pipeline", "Custom Dataset Studio"],
            label_visibility="collapsed",
        )
        st.markdown(
            """
            <p class="korlie-sidebar-label">Current Focus</p>
            <div class="korlie-sidebar-card">
                <p class="korlie-sidebar-copy" style="margin-top:0;">
                    Standard pipeline is optimized for TenkiPay, SLRSA, and GovPay.
                    Custom studio is designed for any other CSV that needs profiling, cleaning, and flexible date filtering.
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
    return workflow


def build_date_filter(
    key_prefix: str,
    *,
    section_label: str | None,
    title: str,
    single_date_label: str,
) -> DateFilterSpec:
    if section_label:
        render_section_label(section_label)

    st.subheader(title)
    date_mode_label = st.radio(
        "Date mode",
        options=["Single Date", "Date Range"],
        horizontal=True,
        key=f"{key_prefix}_date_mode",
    )

    today = pd.Timestamp.today().date()
    if date_mode_label == "Single Date":
        single_date = st.date_input(single_date_label, value=today, key=f"{key_prefix}_single_date")
        return DateFilterSpec(mode="single", single_date=single_date)

    start_col, end_col = st.columns(2)
    start_date = start_col.date_input("Start date", value=today, key=f"{key_prefix}_start_date")
    end_date = end_col.date_input("End date", value=today, key=f"{key_prefix}_end_date")
    return DateFilterSpec(mode="range", start_date=start_date, end_date=end_date)


def collect_rules(
    *,
    title: str,
    columns: list[str],
    key_prefix: str,
    count_label: str,
    no_rule_message: str,
    missing_columns_message: str,
) -> list[ColumnRule]:
    st.markdown(f"### {title}")
    rule_count = int(
        st.number_input(
            count_label,
            min_value=0,
            max_value=10,
            value=0,
            step=1,
            key=f"{key_prefix}_rule_count",
        )
    )

    if rule_count == 0:
        st.caption(no_rule_message)
        return []

    if not columns:
        st.warning(missing_columns_message)
        return []

    rules: list[ColumnRule] = []
    for index in range(rule_count):
        col_a, col_b = st.columns([1, 1])
        column = col_a.selectbox(
            f"{title} column {index + 1}",
            options=columns,
            key=f"{key_prefix}_column_{index}",
        )
        value = col_b.text_input(
            f"{title} value {index + 1}",
            key=f"{key_prefix}_value_{index}",
            help="Exact match. Matching is case-sensitive.",
        )

        if value == "":
            st.caption(f"{title} rule {index + 1} is ignored until a value is entered.")
            continue

        rules.append(ColumnRule(column=column, value=value))

    return rules


def build_profile_dataframe(profile: GenericDatasetProfile) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "column": column.column_name,
                "dtype": column.inferred_dtype,
                "missing_rows": column.missing_count,
                "missing_%": column.missing_percent,
                "unique_values": column.unique_count,
                "sample": column.sample_value or "—",
                "date_candidate": "Yes" if column.is_date_candidate else "",
            }
            for column in profile.columns
        ]
    )


def render_standard_workflow() -> None:
    render_note(
        "Use this workflow for TenkiPay, SLRSA, and GovPay files that already follow the standard Korlie structure and shared `created_at` date logic."
    )

    render_section_label("Upload CSV Files")
    st.subheader("Dataset Intake")
    uploads: dict[str, st.runtime.uploaded_file_manager.UploadedFile | None] = {}
    columns_by_dataset: dict[str, list[str]] = {}
    upload_errors: list[str] = []

    upload_columns = st.columns(3)
    for index, dataset_name in enumerate(DATASET_ORDER):
        uploaded_file = upload_columns[index].file_uploader(
            f"{dataset_name} CSV",
            type=["csv"],
            key=f"{dataset_name}_upload",
        )
        uploads[dataset_name] = uploaded_file

        if uploaded_file is None:
            columns_by_dataset[dataset_name] = []
            continue

        file_bytes = uploaded_file.getvalue()
        columns, error = cached_columns(uploaded_file.name, file_bytes)
        columns_by_dataset[dataset_name] = columns

        if error:
            upload_errors.append(f"{dataset_name}: {error}")
        else:
            upload_columns[index].caption(f"Detected {len(columns)} columns.")

    date_filter = build_date_filter(
        "standard",
        section_label="Shared Date Filter",
        title="Date Selection",
        single_date_label="Transaction date",
    )

    render_section_label("Per-Dataset Filters")
    st.subheader("Exact-Match Rules")
    dataset_filters = [
        DatasetFilterSpec(
            dataset_name=dataset_name,
            rules=collect_rules(
                title=f"{dataset_name} Exact-Match Rules",
                columns=columns_by_dataset[dataset_name],
                key_prefix=f"{dataset_name}_rules",
                count_label=f"Number of {dataset_name} rules",
                no_rule_message="No extra rules for this dataset.",
                missing_columns_message="Upload the CSV first to choose filter columns.",
            ),
        )
        for dataset_name in DATASET_ORDER
    ]

    if upload_errors:
        st.error("\n".join(upload_errors))

    process_clicked = st.button("Process CSV Files", type="primary", use_container_width=True, key="standard_process")
    if not process_clicked:
        return

    uploaded_files = {
        dataset_name: (uploaded_file.name, uploaded_file.getvalue())
        for dataset_name, uploaded_file in uploads.items()
        if uploaded_file is not None
    }

    if not uploaded_files:
        st.error("Upload at least one CSV file before processing.")
        return

    results, errors = process_datasets(
        uploaded_files=uploaded_files,
        date_filter=date_filter,
        dataset_filters=dataset_filters,
    )

    if errors:
        st.error("Validation failed:")
        for error in errors:
            st.write(f"- {error}")
        return

    st.success("Filtering complete.")

    summary_df = pd.DataFrame(
        [
            {
                "dataset": result.dataset_name,
                "input_rows": result.input_rows,
                "output_rows": result.output_rows,
                "invalid_date_rows": result.invalid_date_rows,
                "output_filename": result.output_filename,
            }
            for result in results
        ]
    )
    render_section_label("Results")
    st.subheader("Processing Summary")
    st.dataframe(summary_df, use_container_width=True, hide_index=True)

    st.subheader("Downloads")
    download_columns = st.columns(len(results))
    for index, result in enumerate(results):
        download_columns[index].download_button(
            label=f"Download {result.dataset_name} CSV",
            data=result.csv_bytes,
            file_name=result.output_filename,
            mime="text/csv",
            use_container_width=True,
        )

    zip_bytes = build_results_zip(results)
    st.download_button(
        label="Download ZIP With Filtered CSVs",
        data=zip_bytes,
        file_name="filtered_csv_outputs.zip",
        mime="application/zip",
        use_container_width=True,
    )


def render_custom_workflow() -> None:
    render_note(
        "Use this workflow for any other CSV. The app profiles the dataset, suggests date columns, lets you apply controlled cleaning, and then exports a filtered production-ready file."
    )

    render_section_label("Flexible Dataset Intake")
    st.subheader("Custom Dataset Studio")
    uploaded_file = st.file_uploader(
        "Upload any CSV dataset",
        type=["csv"],
        key="custom_dataset_upload",
        help="This workflow is independent from the fixed TenkiPay, SLRSA, and GovPay pipeline.",
    )

    if uploaded_file is None:
        st.info("Upload a CSV to inspect the schema, configure cleaning rules, and process the file.")
        return

    file_bytes = uploaded_file.getvalue()
    df, profile, error = cached_generic_dataset(uploaded_file.name, file_bytes)
    if error or df is None or profile is None:
        st.error(error or "Could not profile the uploaded dataset.")
        return

    metric_columns = st.columns(5)
    metric_columns[0].metric("Rows", f"{profile.row_count:,}")
    metric_columns[1].metric("Columns", f"{profile.column_count:,}")
    metric_columns[2].metric("Empty Rows", f"{profile.fully_empty_rows:,}")
    metric_columns[3].metric("Duplicate Rows", f"{profile.duplicate_rows:,}")
    metric_columns[4].metric("Date Candidates", f"{len(profile.date_candidates):,}")

    render_section_label("Dataset Understanding")
    st.subheader("Schema Profile")
    if profile.date_candidates:
        st.caption(f"Detected likely date columns: {', '.join(profile.date_candidates)}")
    else:
        st.caption("No strong date columns were detected automatically. You can still choose a column manually if needed.")

    st.dataframe(build_profile_dataframe(profile), use_container_width=True, hide_index=True)

    st.subheader("Dataset Preview")
    st.dataframe(df.head(10), use_container_width=True, hide_index=True)

    render_section_label("Cleaning Controls")
    st.subheader("Data Preparation")
    cleaning_columns = st.columns(3)
    trim_whitespace = cleaning_columns[0].checkbox(
        "Trim whitespace in text fields",
        value=True,
        key="custom_trim_whitespace",
    )
    drop_fully_empty_rows = cleaning_columns[1].checkbox(
        "Remove fully empty rows",
        value=True,
        key="custom_drop_empty_rows",
    )
    drop_duplicate_rows = cleaning_columns[2].checkbox(
        "Remove duplicate rows",
        value=True,
        key="custom_drop_duplicate_rows",
    )
    required_columns = st.multiselect(
        "Columns that must contain values",
        options=list(df.columns),
        key="custom_required_columns",
        help="Rows missing values in any selected column will be removed before filtering.",
    )
    cleaning_spec = CleaningSpec(
        trim_whitespace=trim_whitespace,
        drop_fully_empty_rows=drop_fully_empty_rows,
        drop_duplicate_rows=drop_duplicate_rows,
        required_columns=required_columns,
    )

    render_section_label("Flexible Date Filter")
    st.subheader("Date Filtering")
    apply_date_filter = st.checkbox(
        "Apply date filtering to this custom dataset",
        value=bool(profile.date_candidates),
        key="custom_apply_date_filter",
    )

    date_column: str | None = None
    date_filter: DateFilterSpec | None = None
    if apply_date_filter:
        date_column_options = profile.date_candidates or list(df.columns)
        date_column = st.selectbox(
            "Date column",
            options=date_column_options,
            key="custom_date_column",
            help="Choose the column that should drive single-date or date-range filtering.",
        )
        date_filter = build_date_filter(
            "custom",
            section_label=None,
            title="Date Selection",
            single_date_label="Filter date",
        )
    else:
        st.caption("Date filtering is optional in the custom workflow. Leave it off when you only need cleaning or exact-match filtering.")

    render_section_label("Targeted Filters")
    st.subheader("Exact-Match Rules")
    rules = collect_rules(
        title="Custom Dataset Rules",
        columns=list(df.columns),
        key_prefix="custom_rules",
        count_label="Number of custom rules",
        no_rule_message="No extra exact-match rules for this dataset.",
        missing_columns_message="Upload a CSV first to choose filter columns.",
    )

    process_clicked = st.button(
        "Process Custom Dataset",
        type="primary",
        use_container_width=True,
        key="custom_process",
    )
    if not process_clicked:
        return

    result, errors = process_generic_dataset(
        dataset_name=Path(uploaded_file.name).stem,
        original_filename=uploaded_file.name,
        file_bytes=file_bytes,
        cleaning_spec=cleaning_spec,
        rules=rules,
        apply_date_filter=apply_date_filter,
        date_column=date_column,
        date_filter=date_filter,
    )

    if errors or result is None:
        st.error("Validation failed:")
        for error_message in errors:
            st.write(f"- {error_message}")
        return

    st.success("Custom dataset processed successfully.")

    render_section_label("Results")
    st.subheader("Processing Summary")
    summary_df = pd.DataFrame(
        [
            {
                "dataset": result.dataset_name,
                "input_rows": result.input_rows,
                "rows_after_cleaning": result.rows_after_cleaning,
                "output_rows": result.output_rows,
                "invalid_date_rows": result.invalid_date_rows,
                "removed_empty_rows": result.removed_empty_rows,
                "removed_duplicate_rows": result.removed_duplicate_rows,
                "removed_missing_required_rows": result.removed_missing_required_rows,
                "output_filename": result.output_filename,
            }
        ]
    )
    st.dataframe(summary_df, use_container_width=True, hide_index=True)

    processed_df, processed_error = load_csv_dataframe(result.csv_bytes)
    if processed_error is None and processed_df is not None:
        st.subheader("Processed Preview")
        st.dataframe(processed_df.head(10), use_container_width=True, hide_index=True)

    st.subheader("Download")
    st.download_button(
        label="Download Processed Custom CSV",
        data=result.csv_bytes,
        file_name=result.output_filename,
        mime="text/csv",
        use_container_width=True,
    )


def main() -> None:
    inject_styles()
    render_hero()
    workflow = render_sidebar()

    if workflow == "Standard Dataset Pipeline":
        render_standard_workflow()
    else:
        render_custom_workflow()

    render_footer()


if __name__ == "__main__":
    main()
