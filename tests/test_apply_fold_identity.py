"""fold_batch marks each event's full_old from ITS OWN schema version's replica identity,
also when versions with different identities interleave in one batch (the per-call
identity memo must key on the version, and never outlive the call)."""

import json

from cdc_flight.apply import Apply

COLUMNS = [[1, "id", 25, -1], [0, "v", 25, -1]]
DESCRIPTORS = {
    "vf": json.dumps({"identity": "f", "columns": COLUMNS}),
    "vi": json.dumps({"identity": "i", "columns": COLUMNS}),
}


def event(lsn, version, op="insert"):
    new = {"id": {"kind": "value", "value": str(lsn)}, "v": {"kind": "value", "value": "x"}}
    return dict(
        commit_lsn=lsn,
        commit_ts_us=0,
        op=op,
        schema_version=version,
        payload={"old": None, "new": None if op == "truncate" else new},
    )


def test_full_old_follows_each_events_own_version():
    events = [event(1, "vi"), event(2, "vf"), event(3, "vi"), event(4, "vf")]
    current = {"columns": None, "keys": ["id"], "epoch": 1}
    usable, _, images, blocked = Apply.fold_batch(
        object(), 101, current, events, descriptors=DESCRIPTORS
    )
    assert blocked is None and usable is events and len(images) == 4
    assert [e["full_old"] for e in events] == [False, True, False, True]


def test_a_truncate_is_never_full_old_and_blocks():
    events = [event(1, "vf"), event(2, None, op="truncate")]
    current = {"columns": None, "keys": ["id"], "epoch": 1}
    usable, _, _, blocked = Apply.fold_batch(
        object(), 101, current, events, descriptors=DESCRIPTORS
    )
    assert blocked is not None and [e["commit_lsn"] for e in usable] == [1]
    assert events[0]["full_old"] is True
