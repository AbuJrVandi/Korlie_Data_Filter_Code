# CSV Automation

Local Streamlit app with two workflows:
- a fixed pipeline for `TenkiPay`, `SLRSA`, and `GovPay`
- a custom dataset studio for any other CSV

## Folder

```text
csv_automation/
  app.py
  engine.py
  models.py
  zip_utils.py
  requirements.txt
  README.md
  outputs/
```

## Requirements

- Python 3.9+
- `pip`

## Install

```bash
cd /Users/user/Desktop/Korlie_Data/csv_automation
python3 -m pip install -r requirements.txt
```

## Run

```bash
cd /Users/user/Desktop/Korlie_Data/csv_automation
streamlit run app.py
```

## Workflow

### Standard Dataset Pipeline

1. Upload one or more CSV files in the labeled inputs.
2. Choose `Single Date` or `Date Range`.
3. Add optional exact-match rules per dataset.
4. Click `Process CSV Files`.
5. Download each filtered CSV or the ZIP bundle.

TenkiPay-specific behavior:
- TenkiPay is automatically filtered to `paidto == "WANGOV"` in addition to the shared date filter and any optional exact-match rules.
- This rule is built into the standard pipeline and is not exposed as a separate UI control.

### Custom Dataset Studio

1. Open `Custom Dataset Studio` from the sidebar and upload any CSV file.
2. Review the schema profile, preview, and inferred date columns.
3. Choose optional cleaning controls such as trimming whitespace, removing empty rows, removing duplicates, and requiring values in selected columns.
4. Optionally enable date filtering with a selected date column and `Single Date` or `Date Range`.
5. Add optional exact-match rules.
6. Click `Process Custom Dataset`.
7. Download the processed CSV.

## Validation Rules

- At least one file is required.
- Each CSV must include `created_at`.
- TenkiPay uploads in the standard pipeline must also include `paidto`.
- Requested filter columns must exist in the target dataset.
- Date ranges must be valid.
- Invalid `created_at` values are excluded from filtered results and counted in the summary.

## Notes

- Existing scripts in `TenkiPay`, `SLRSA_data`, and `GovPay_data` are unchanged.
- Exact-match filters are case-sensitive.
- The built-in TenkiPay transaction-type filter is also exact and case-sensitive: `paidto == "WANGOV"`.
- The custom workflow infers likely date columns from column names and sample values, but users can still choose a different column manually.
- v1 does not support plain-English prompts, fuzzy matching, numeric ranges, or OR logic.
