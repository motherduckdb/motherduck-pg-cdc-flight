"""Keyless capture's format-2 golden: an append relation's frames
carry an empty key layout and width-0 key images. Format 2 itself is unchanged, and its
golden (fixtures/format2-protocol.json) is untouched; this fixture pins what an append
table's unit looks like under it: the empty key layout's hash, every chunk's digest and
descriptor keys, and the unit's terminal digest, from the real Spool."""

import json
from pathlib import Path

import pgstream
from test_format2_protocol import binding, config

from cdc_flight import format2 as f2
from cdc_flight.format2 import UnitConfig
from cdc_flight.ingest_log2 import IngestConfig2
from cdc_flight.spool2 import Spool

GOLDEN = json.loads((Path(__file__).parent / "fixtures/format2-keyless.json").read_text())


def keyless_unit():
    """Two commits of a keyless FULL relation: duplicate inserts; TRUNCATE then an insert
    with a NULL cell. One unit (max_commits=2), three frames per chunk."""
    cfg = IngestConfig2(
        **config().__dict__,
        text_binding=binding(),
        unit_config=UnitConfig(max_commits=2, chunk_frames=3),
    )
    spool = Spool(
        cfg,
        {201: (), 900: ("id",)},
        900,
        "marker",
        binding=cfg.text_binding,
        start_lsn=90,
        unit_config=cfg.unit_config,
        clock=lambda: 0,
    )
    columns = ((0, "at", 25, -1), (0, "msg", 25, -1))
    messages = [
        pgstream.relation(201, "public", "log", "f", columns),
        pgstream.begin(100, -5, 7),
        pgstream.insert(201, ("t1", "dup")),
        pgstream.insert(201, ("t1", "dup")),
        pgstream.commit(100, 105, -4),
        pgstream.begin(110, -5, 8),
        pgstream.truncate([201]),
        pgstream.insert(201, ("t2", None)),
        pgstream.commit(110, 115, -4),
    ]
    return [i for m in messages for i in spool.feed(m)] + list(spool.close())


def test_the_empty_key_layout_and_projection():
    assert f2.key_layout(()).hex() == GOLDEN["key_layout"]
    assert f2.key_layout(()) != f2.key_layout(("id",))
    image = {"values": ["t1", "dup"], "kinds": [f2.TEXT, f2.TEXT]}
    empty = f2.project(image, ())
    assert empty == {"values": [], "kinds": []}
    f2.validate_image(empty, 0)
    f2.validate_projection(image, empty, ())


def test_a_keyless_unit_is_golden():
    items = keyless_unit()
    chunks = [
        dict(
            id=i.id,
            digest=i.digest,
            chain_digest=i.chain_digest,
            keys=[list(d.keys) for d in i.descriptors],
        )
        for i in items
        if isinstance(i, f2.Chunk)
    ]
    units = [
        dict(start=i.unit_start_lsn, end=i.end_lsn, digest=i.digest, chain_digest=i.chain_digest)
        for i in items
        if isinstance(i, f2.UnitComplete)
    ]
    assert chunks == GOLDEN["chunks"] and units == GOLDEN["units"]
    assert all(keys == [[]] for keys in (c["keys"] for c in chunks))
