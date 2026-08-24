from __future__ import annotations

from dataclasses import dataclass
from queue import Empty, SimpleQueue
from threading import Event, Thread
from typing import Protocol

from hepflow.progress.model import ProgressUpdate


class ProgressSink(Protocol):
    def handle(self, update: ProgressUpdate) -> None:
        ...


class CloseableProgressSink(ProgressSink, Protocol):
    def close(self) -> None:
        ...


class NullProgressSink:
    def handle(self, update: ProgressUpdate) -> None:
        del update


@dataclass(frozen=True, slots=True)
class ProgressSinkWarning:
    sink: str
    message: str

    def to_dict(self) -> dict[str, str]:
        return {"sink": self.sink, "message": self.message}


class BufferedProgressSink:
    def __init__(self, sink: ProgressSink, *, name: str | None = None) -> None:
        self.sink = sink
        self.name = name or type(sink).__name__
        self._queue: SimpleQueue[ProgressUpdate | None] = SimpleQueue()
        self._stopped = Event()
        self._disabled = Event()
        self._warnings: list[ProgressSinkWarning] = []
        self._thread = Thread(
            target=self._worker,
            name=f"hepflow-progress-{self.name}",
            daemon=True,
        )
        self._thread.start()

    @property
    def warnings(self) -> list[ProgressSinkWarning]:
        return list(self._warnings)

    def enqueue(self, update: ProgressUpdate) -> None:
        if not self._disabled.is_set():
            self._queue.put(update)

    def close(self, *, timeout: float = 2.0) -> None:
        self._queue.put(None)
        self._thread.join(timeout=timeout)
        if self._thread.is_alive():
            self._warnings.append(
                ProgressSinkWarning(
                    sink=self.name,
                    message=f"progress sink did not stop within {timeout:.3g}s",
                )
            )
            return
        close = getattr(self.sink, "close", None)
        if callable(close):
            try:
                close()
            except Exception as exc:
                self._warnings.append(
                    ProgressSinkWarning(
                        sink=self.name,
                        message=f"progress sink close failed: {type(exc).__name__}: {exc}",
                    )
                )

    def _worker(self) -> None:
        while not self._stopped.is_set():
            try:
                update = self._queue.get(timeout=0.1)
            except Empty:
                continue
            if update is None:
                self._stopped.set()
                return
            if self._disabled.is_set():
                continue
            try:
                self.sink.handle(update)
            except Exception as exc:
                self._disabled.set()
                self._warnings.append(
                    ProgressSinkWarning(
                        sink=self.name,
                        message=f"progress sink failed: {type(exc).__name__}: {exc}",
                    )
                )
