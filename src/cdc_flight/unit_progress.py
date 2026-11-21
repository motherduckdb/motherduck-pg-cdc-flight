"""In-memory collected frame progress; no stream or database dependencies."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Progress:
    epoch: int
    unit_anchor: int
    active_commit: int | None
    durable_frame_end: int
