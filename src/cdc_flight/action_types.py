"""The action results and input helpers shared by the table-action core and every action
Flight, with no import beyond config: an action Flight can name them without pulling
apply or table_actions into its imports. table_actions re-exports these exact
objects, so `table_actions.Refused is action_types.Refused`.
"""

import re
from dataclasses import dataclass, field

from cdc_flight.config import lsn_text


@dataclass(frozen=True)
class ActionResult:
    status: str  # done, already, refused, busy, would (a dry run)
    table: str
    detail: str = ""
    next_steps: list = field(default_factory=list)
    observed: dict = field(default_factory=dict)  # what was read: for previews


class Refused(Exception):
    """A `consume` hook's refusal inside the action's transaction (an action Flight's
    preview): roll back and report the message."""


def lsn_or_none(lsn):
    return "none" if lsn is None else lsn_text(lsn)


def parse_lsn(text):
    """`X/Y` hex, as PostgreSQL prints an LSN; any case, leading zeros allowed."""
    if not re.fullmatch(r"[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}", text or ""):
        raise ValueError('an LSN is "X/Y" in hex, for example 0/5A3B2C10')
    high, low = text.split("/")
    return int(high, 16) << 32 | int(low, 16)
