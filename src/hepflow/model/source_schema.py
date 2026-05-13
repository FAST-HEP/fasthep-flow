from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(frozen=True)
class BranchSchema:
    index: int
    name: str
    typename: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ObjectSchema:
    path: str
    path_raw: str
    type: str
    entries: int
    schema_signature: str
    branches: list[BranchSchema] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "path_raw": self.path_raw,
            "type": self.type,
            "entries": self.entries,
            "schema_signature": self.schema_signature,
            "branches": [b.to_dict() for b in self.branches],
        }


@dataclass(frozen=True)
class FileSchema:
    version: int
    dataset: str
    file_index: int
    file: dict[str, Any]
    inspected_at: str
    objects: list[ObjectSchema] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "dataset": self.dataset,
            "file_index": self.file_index,
            "file": self.file,
            "inspected_at": self.inspected_at,
            "objects": [o.to_dict() for o in self.objects],
        }

    @staticmethod
    def from_dict(d: dict[str, Any]) -> FileSchema:
        return FileSchema(
            version=int(d["version"]),
            dataset=str(d["dataset"]),
            file_index=int(d["file_index"]),
            file=dict(d["file"]),
            inspected_at=str(d["inspected_at"]),
            objects=[
                ObjectSchema(
                    path=str(o["path"]),
                    path_raw=str(o.get("path_raw", o["path"])),
                    type=str(o["type"]),
                    entries=int(o["entries"]),
                    schema_signature=str(o["schema_signature"]),
                    branches=[
                        BranchSchema(
                            index=int(b["index"]),
                            name=str(b["name"]),
                            typename=str(b["typename"]),
                        )
                        for b in (o.get("branches") or [])
                    ],
                )
                for o in (d.get("objects") or [])
            ],
        )


def make_schema_signature(
    *,
    object_path: str,
    object_type: str,
    branches: list[BranchSchema],
) -> str:
    payload = {
        "path": object_path,
        "type": object_type,
        "branches": [
            {"name": b.name, "typename": b.typename}
            for b in sorted(branches, key=lambda x: x.name)
        ],
    }
    raw = repr(payload).encode("utf-8")
    return "sha256:" + sha256(raw).hexdigest()


def _unix_to_iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, UTC).isoformat()

def make_file_schema(
    *,
    dataset: str,
    file_index: int,
    file_path: str,
    objects: list[ObjectSchema],
) -> FileSchema:
    path = Path(file_path)
    st = path.stat()
    return FileSchema(
        version=1,
        dataset=dataset,
        file_index=file_index,
        file={
            "path": file_path,
            "name": path.name,
            "format": "root",
            "size_bytes": int(st.st_size),
            "mtime_unix": float(st.st_mtime),
            "mtime_iso": _unix_to_iso(float(st.st_mtime)),
        },
        inspected_at=_now_iso(),
        objects=objects,
    )
