"""Command-line entrypoint for the deduplication tool."""

from dedup_tool.main import run_cli


if __name__ == "__main__":
    raise SystemExit(run_cli())
