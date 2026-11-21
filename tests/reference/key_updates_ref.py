"""FROZEN per-table format-2 key update at 0a03f26, before R7 batching.

Only the stored-row result is the oracle; cut/activation helpers stay shared and
are independently tested for malformed cuts, epochs, queues and retention.
"""

import json

from cdc_flight.ingest_common import VALIDATION_HOLDS, SourceMismatch
from cdc_flight.unit_store import encoded


def record_keys(self, observed, boundary, checked_lsn):
    self._validate_keys(observed)
    self._key_cut(self.epoch, boundary)
    position = self.unit_position()
    if position["source_epoch"] != self.epoch or boundary > position["confirmed_end"]:
        raise SourceMismatch("key verification exceeds the durable unit boundary")
    initial = self.execute(
        "SELECT start_lsn FROM {s}source_epochs WHERE epoch=?", [self.epoch]
    ).fetchone()[0]
    rows = self.execute(
        "SELECT relation_id,key_columns,catalog_key,key_checked_lsn,key_epoch,"
        "pending_keys_v2,held_reason FROM {s}tables"
    ).fetchall()
    for oid, registered, catalog, checked, key_epoch, payload, held in rows:
        if oid not in observed:
            continue
        keys = list(observed[oid]) if observed[oid] is not None else None
        queue = self._pending(payload)
        known = queue[-1]["keys"] if queue else json.loads(catalog or registered)
        cut = (
            queue[0]["cut_lsn"]
            if queue
            else (checked if checked is not None and key_epoch in (None, self.epoch) else initial)
        )
        self._key_cut(self.epoch, cut)
        if keys != known or (oid in self._valid_again and not queue):
            queue.append(dict(epoch=self.epoch, keys=keys, cut_lsn=cut, observed_lsn=checked_lsn))
            self.execute(
                "UPDATE {s}tables SET pending_keys_v2=? WHERE relation_id=?",
                [encoded(queue), oid],
            )
        elif not queue and held not in VALIDATION_HOLDS:
            self.execute(
                "UPDATE {s}tables SET key_checked_lsn=?,key_epoch=? WHERE relation_id=?",
                [max(cut, boundary), self.epoch, oid],
            )
        # With a queue, do not advance key_checked_lsn: Apply2 must stop at
        # the PRIOR verified cut even while this run drains with old keys.
    return self._activate_keys()
