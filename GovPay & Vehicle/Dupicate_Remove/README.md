# Intelligent Dataset Deduplication Tool

This folder contains a production-style deduplication tool for messy government/admin data.

It automatically:
- detects all CSV/Excel files in this folder,
- normalizes fields (names, phones, IDs, dates, punctuation, hidden characters),
- detects exact and fuzzy duplicates,
- keeps the most complete record,
- writes cleaned datasets and duplicate reports.

## Folder Layout

```text
Dupicate_Remove/
├── deduplicate.py
├── requirements.txt
├── README.md
├── dedup_tool/
│   ├── loader.py
│   ├── normalizer.py
│   ├── matcher.py
│   ├── resolver.py
│   ├── reporter.py
│   └── main.py
├── cleaned/   (generated)
├── report/    (generated)
└── logs/      (generated)
```

## How to Use

1. Open terminal in:
   `/Users/user/Desktop/Korlie_Data/GovPay & Vehicle/Dupicate_Remove`
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Put your input files (`.csv`, `.xlsx`, `.xls`) in this folder.
4. Run the tool:

```bash
python deduplicate.py --threshold 88
```

If you omit `--threshold`, default is `85`.

## Outputs

For each input file (example: `payments.xlsx`), the tool creates:

- `cleaned/payments_cleaned.csv`
- `report/payments_duplicate_report.csv`

Log file:
- `logs/cleaning.log`

## Duplicate Logic (High Level)

The matcher combines multiple strategies:
- exact full-row match,
- exact match on strong columns (e.g. phone, ID, email),
- fuzzy name + date matching,
- row-level similarity score using weighted field matching.

Rows with score >= threshold are clustered as duplicates.
Within each duplicate cluster, the tool keeps the most complete row.

## Notes

- Cleaned outputs are always CSV for consistency.
- Original files are never modified.
- Duplicate report includes original row, matched row, score, and detection reason.
