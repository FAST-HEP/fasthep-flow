from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Any


class IssueLevel(enum.IntEnum):
    ERROR = 0
    WARN = 1
    INFO = 2
    DEBUG = 3


@dataclass(frozen=True)
class FlowIssue:
    code: str
    message: str
    meta: dict[str, Any]
    level: IssueLevel = IssueLevel.INFO

    def format(self) -> str:
        head = f"[{self.level.name}] {self.code}: {self.message}"
        if not self.meta:
            return head
        meta_str = ", ".join(f"{k}={v!r}" for k, v in self.meta.items())
        return f"{head}\n  meta: {meta_str}"

    def is_error(self) -> bool:
        return self.level == IssueLevel.ERROR

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "meta": self.meta,
            "level": self.level.name.lower(),
        }

    @staticmethod
    def from_dict(d: dict[str, Any]) -> FlowIssue:
        return FlowIssue(
            code=str(d["code"]),
            message=str(d["message"]),
            meta=dict(d.get("meta") or {}),
            level=IssueLevel[str(d.get("level", "info")).upper()],
        )
