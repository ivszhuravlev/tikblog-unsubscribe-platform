import csv
import io
from collections.abc import Iterator
from contextlib import closing
from dataclasses import dataclass

from tikblog_storage import ObjectStore, as_prefix

REJECT_HEADER = ["source_file", "line_number", "raw_row", "reject_reason"]


@dataclass(frozen=True, slots=True)
class Reject:
    line_number: int
    # Keep cells separate so quoted commas remain valid CSV.
    raw_row: list[str]
    reason: str

    def raw_row_as_csv(self) -> str:
        buffer = io.StringIO()
        csv.writer(buffer, lineterminator="").writerow(self.raw_row)
        return buffer.getvalue()


class LandingZone:
    """Read and move Legal batches inside one MinIO bucket."""

    def __init__(self, store: ObjectStore, inbox: str, processed: str, rejects: str) -> None:
        self._store = store
        self._inbox = as_prefix(inbox)
        self._processed = as_prefix(processed)
        self._rejects = as_prefix(rejects)

    def list_batches(self) -> list[str]:
        """List CSV files directly inside the inbox."""
        names = []

        for key in self._store.list_keys(self._inbox):
            name = key[len(self._inbox) :]

            if "/" in name or not name.lower().endswith(".csv"):
                continue

            names.append(name)

        return sorted(names)

    def already_processed(self, name: str) -> bool:
        """Check whether a file with this name was processed."""
        return self._exists(f"{self._processed}{name}")

    def read_rows(self, name: str) -> Iterator[list[str]]:
        """Read a CSV one row at a time without loading the whole file."""
        with closing(self._store.open_stream(f"{self._inbox}{name}")) as stream:
            # utf-8-sig also accepts Excel CSV files with a BOM.
            text = io.TextIOWrapper(stream, encoding="utf-8-sig", newline="")
            yield from csv.reader(text)

    def write_rejects(self, name: str, rejects: list[Reject]) -> None:
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(REJECT_HEADER)

        for reject in rejects:
            writer.writerow([name, reject.line_number, reject.raw_row_as_csv(), reject.reason])

        # Object storage writes the reject file in one call.
        self._store.write(f"{self._rejects}{name}.rejects.csv", buffer.getvalue().encode("utf-8"))

    def mark_processed(self, name: str) -> None:
        """Move a published batch from inbox to processed."""
        self._store.move(f"{self._inbox}{name}", f"{self._processed}{name}")

    def _exists(self, key: str) -> bool:
        """Check for an exact key, not just a matching prefix."""
        return key in self._store.list_keys(key)
