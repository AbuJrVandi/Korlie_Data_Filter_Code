from __future__ import annotations

from io import BytesIO
from zipfile import ZIP_DEFLATED, ZipFile

from models import DatasetResult


def build_results_zip(results: list[DatasetResult]) -> bytes:
    buffer = BytesIO()

    with ZipFile(buffer, mode="w", compression=ZIP_DEFLATED) as zip_file:
        for result in results:
            zip_file.writestr(result.output_filename, result.csv_bytes)

    buffer.seek(0)
    return buffer.getvalue()
