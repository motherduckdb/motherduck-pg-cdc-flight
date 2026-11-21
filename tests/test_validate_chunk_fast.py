"""validate_chunk's columnar fast path against the exact row-by-row validator.

The fast path may only skip the exact validator's cost for a chunk it proves valid; every
other chunk goes to the exact validator. So for any chunk, valid or corrupted:
- if the exact validator raises, validate_chunk raises the identical error;
- if it accepts, validate_chunk returns the same rows on the fields `append` reads.
Chunks come from the real spool over varied wire; each mutation class targets one
invariant the fast path re-checks. CDC_TEST_VALIDATE_FUZZ=<n> runs n mutants (default 600).
"""

import os
import random
from dataclasses import replace

import pgstream
import pyarrow as pa
import pytest
from test_format2_protocol import binding, config
from test_spool2 import relation_for

from cdc_flight import format2 as f2
from cdc_flight import ingest_log2 as L
from cdc_flight.spool2 import Spool

N = int(os.getenv("CDC_TEST_VALIDATE_FUZZ", "600"))
COLS = ((1, "id", 25, -1), (0, "v", 25, -1))
KEY_ONLY = pgstream.relation(103, "public", "items103", "d", COLS)


def _wire(rng):
    """A stream over three relations: inserts, FULL and key-only updates, deletes, NULLs,
    unchanged TOAST, single-row and multi-row transactions, and a few TRUNCATEs."""
    messages = [relation_for(101), relation_for(102), KEY_ONLY]
    lsn = 1000
    for i in range(160):
        lsn += 64
        body = []
        for _ in range(rng.choice([1, 1, 2, 5])):
            oid = rng.choice([101, 102, 103])
            k = str(rng.randrange(100))
            v = rng.choice(["v" * rng.randrange(1, 40), None, 'é\n"\\'])
            op = rng.random()
            if op < 0.5:
                body.append(pgstream.insert(oid, (k, v)))
            elif op < 0.8:
                if oid == 103:
                    body.append(pgstream.update(oid, (k, pgstream.UNCHANGED), (k, None), "K"))
                else:
                    body.append(pgstream.update(oid, (k, v), (k, "old"), "O"))
            else:
                old = (k, None) if oid == 103 else (k, "old")
                body.append(pgstream.delete(oid, old, "K" if oid == 103 else "O"))
        if i % 40 == 39:
            body.append(pgstream.truncate((101, 102), options=i % 4))
        messages += [pgstream.begin(lsn, 1_000 + i, 7 + i), *body, pgstream.commit(lsn, lsn + 1, 1)]
    return messages


def _chunks(seed):
    rng = random.Random(seed)
    spool = Spool(
        config(),
        {101: ("id",), 102: ("id",), 103: ("id",)},
        900,
        "marker",
        binding=binding(),
        start_lsn=900,
        unit_config=f2.UnitConfig(max_commits=6, chunk_frames=24),
        clock=lambda: 0,
    )
    out = []
    for wire in _wire(rng):
        out += [item for item in spool.feed(wire) if isinstance(item, f2.Chunk)]
    out += [item for item in spool.close() if isinstance(item, f2.Chunk)]
    return out


def _outcome(validate, chunk):
    try:
        rows = validate(chunk)
    except ValueError as exc:
        return ("err", str(exc))
    return ("ok", [{k: r[k] for k in L._ROW_FIELDS} for r in rows])


def _set(chunk, index, name, value):
    rows = chunk.table.to_pylist()
    rows[index][name] = value
    try:
        table = pa.Table.from_pylist(rows, schema=f2.FRAME_SCHEMA)
    except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError, TypeError):
        return None
    return replace(chunk, table=table)


def _set_many(chunk, index, changes):
    rows = chunk.table.to_pylist()
    rows[index].update(changes)
    try:
        table = pa.Table.from_pylist(rows, schema=f2.FRAME_SCHEMA)
    except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError, TypeError):
        return None
    return replace(chunk, table=table)


def _values_shift(rng, chunk):
    """One image's values one longer and another's one shorter, kinds untouched: the
    flattened lengths still agree, so only the per-row values-length check catches it."""
    column = rng.choice(["old", "new"])
    rows = chunk.table.to_pylist()
    picks = [i for i, r in enumerate(rows) if _event(r) and r[column] is not None]
    if len(picks) < 2:
        return None
    a, b = rng.sample(picks, 2)
    rows[a][column] = dict(rows[a][column], values=rows[a][column]["values"] + ["x"])
    rows[b][column] = dict(rows[b][column], values=rows[b][column]["values"][:-1])
    try:
        return replace(chunk, table=pa.Table.from_pylist(rows, schema=f2.FRAME_SCHEMA))
    except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError, TypeError):
        return None


def _absent_in_source(rng, chunk):
    """ABSENT in a non-key cell of an old/new image: valid as a cell, invalid in a source tuple."""
    column = rng.choice(["old", "new"])
    picks = _rows_where(chunk, lambda r: _event(r) and r[column] is not None)
    if not picks:
        return None
    i = rng.choice(picks)
    image = chunk.table.to_pylist()[i][column]
    kinds, values = list(image["kinds"]), list(image["values"])
    kinds[-1], values[-1] = 0, None  # column "v": never a key here
    return _set(chunk, i, column, {"values": values, "kinds": kinds})


def _projection_kind(rng, chunk):
    """Key cell and image cell both carry no value but different kinds (NULL vs UNCHANGED):
    each image is valid alone; only the projection's kind comparison differs."""
    column = rng.choice(["old", "new"])
    key = column + "_key"
    picks = _rows_where(chunk, lambda r: _event(r) and r[column] is not None and r[key] is not None)
    if not picks:
        return None
    i = rng.choice(picks)
    row = chunk.table.to_pylist()[i]
    image = {"values": [None] + row[column]["values"][1:], "kinds": [1] + row[column]["kinds"][1:]}
    keyed = {"values": [None] + row[key]["values"][1:], "kinds": [2] + row[key]["kinds"][1:]}
    return _set_many(chunk, i, {column: image, key: keyed})


def _wire_overflow(rng, chunk):
    """Two event wire_lengths raised by 2**63 each: every value is a valid uint64, and
    their uint64 sum wraps back to the chunk's byte count."""
    rows = chunk.table.to_pylist()
    picks = [i for i, r in enumerate(rows) if _event(r)]
    if len(picks) < 2:
        return None
    for i in rng.sample(picks, 2):
        rows[i]["wire_length"] += 1 << 63
    try:
        return replace(chunk, table=pa.Table.from_pylist(rows, schema=f2.FRAME_SCHEMA))
    except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError, TypeError):
        return None


def _rows_where(chunk, predicate):
    return [i for i, r in enumerate(chunk.table.to_pylist()) if predicate(r)]


def _event(r):
    return r["record_kind"] == f2.EVENT and r["op"] != "truncate"


def _image(rng, chunk, column, change):
    picks = _rows_where(chunk, lambda r: _event(r) and r[column] is not None)
    if not picks:
        return None
    i = rng.choice(picks)
    image = dict(chunk.table.to_pylist()[i][column])
    image = change(rng, {"values": list(image["values"]), "kinds": list(image["kinds"])})
    return _set(chunk, i, column, image)


def _cell(rng, image):
    j = rng.randrange(len(image["kinds"]))
    image["kinds"][j], image["values"][j] = rng.choice(
        [(0, None), (4, None), (3, None), (1, "x"), (2, "x"), (image["kinds"][j], "other")]
    )
    return image


MUTATIONS = {
    "uint_null": lambda rng, c: _set(
        c, rng.randrange(c.count), rng.choice(L._UINT_COLUMNS + ("commit_ts_us",)), None
    ),
    "ordinal": lambda rng, c: _set(
        c,
        i := rng.randrange(c.count),
        "frame_ordinal",
        c.first_frame + i + rng.choice([1, -1 if i else 2, 7]),
    ),
    "unit_or_epoch": lambda rng, c: _set(
        c, rng.randrange(c.count), rng.choice(["unit_start_lsn", "source_epoch"]), 5
    ),
    "record_kind": lambda rng, c: _set(
        c, rng.randrange(c.count), "record_kind", rng.choice([0, 3])
    ),
    "op": lambda rng, c: _set(
        c,
        rng.randrange(c.count),
        "op",
        rng.choice(["insert", "update", "delete", "commit", "upsert", None]),
    ),
    "old_tuple_kind": lambda rng, c: _set(
        c, rng.randrange(c.count), "old_tuple_kind", rng.choice([0, 1, 2, 3, None])
    ),
    "image_presence": lambda rng, c: _set(
        c, rng.randrange(c.count), rng.choice(["old", "new", "old_key", "new_key"]), None
    ),
    "image_width": lambda rng, c: _image(
        rng,
        c,
        rng.choice(["old", "new", "old_key", "new_key"]),
        lambda r, im: {"values": im["values"] + ["w"], "kinds": im["kinds"] + [3]},
    ),
    "cell": lambda rng, c: _image(rng, c, rng.choice(["old", "new", "old_key", "new_key"]), _cell),
    "projection": lambda rng, c: _image(
        rng,
        c,
        rng.choice(["old_key", "new_key"]),
        lambda r, im: {"values": ["other"] + im["values"][1:], "kinds": [3] + im["kinds"][1:]},
    ),
    "descriptor": lambda rng, c: (
        _set(
            c,
            i,
            rng.choice(["schema_name", "table_name", "schema_version", "key_layout_hash"]),
            "wrong",
        )
        if (picks := _rows_where(c, _event)) and (i := rng.choice(picks)) is not None
        else None
    ),
    "commit_frame": lambda rng, c: (
        _set(
            c,
            i,
            *rng.choice(
                [
                    ("wire_length", 27),
                    ("end_lsn", 0),
                    ("transaction_digest", "A" * 64),
                    ("transaction_digest", "0" * 63),
                    ("new", {"values": [], "kinds": []}),
                ]
            ),
        )
        if (picks := _rows_where(c, lambda r: r["record_kind"] == f2.COMMIT))
        and (i := rng.choice(picks)) is not None
        else None
    ),
    "truncate": lambda rng, c: (
        _set(
            c,
            i,
            *rng.choice(
                [
                    ("truncate_oids", []),
                    ("truncate_options", 4),
                    ("relation_id", 101),
                    ("truncate_oids", [999]),
                ]
            ),
        )
        if (picks := _rows_where(c, lambda r: r["op"] == "truncate"))
        and (i := rng.choice(picks)) is not None
        else None
    ),
    "values_shift": _values_shift,
    "wire_overflow": _wire_overflow,
    "absent_in_source": _absent_in_source,
    "projection_kind": _projection_kind,
    "accounting": lambda rng, c: replace(
        c,
        **{
            (f := rng.choice(["event_count", "member_count", "bytes"])): getattr(c, f)
            + rng.choice([1, -1])
        },
    ),
    "chunk_identity": lambda rng, c: replace(
        c,
        **rng.choice(
            [
                dict(first_frame=c.first_frame + 1),
                dict(count=c.count + 1),
                dict(digest="Z" * 64),
                dict(epoch=c.epoch + 1),
            ]
        ),
    ),
}


@pytest.fixture(scope="module")
def chunks():
    out = _chunks(7) + _chunks(8)
    assert any(r["op"] == "truncate" for c in out for r in c.table.to_pylist())
    return out


def test_the_fast_path_takes_valid_chunks_and_matches_the_exact_validator(chunks):
    before = dict(L.VALIDATION_STATS)
    truncating = 0
    for chunk in chunks:
        assert _outcome(L.validate_chunk, chunk) == _outcome(L._validate_chunk_rows, chunk)
        truncating += any(r["op"] == "truncate" for r in chunk.table.to_pylist())
    fast = L.VALIDATION_STATS["fast"] - before["fast"]
    exact = L.VALIDATION_STATS["exact"] - before["exact"]
    # Every valid TRUNCATE-free chunk takes the fast path; TRUNCATE chunks go exact.
    assert (fast, exact) == (len(chunks) - truncating, truncating)


def test_golden_and_builder_chunks_take_the_fast_path():
    from test_spool2 import REL, collect, spool, transaction

    output = collect(spool(), [REL, *transaction(100, ("1", 'é\n"\\'))])
    output += collect(
        spool(),
        [
            REL,
            *transaction(change=pgstream.update(101, ("1", pgstream.UNCHANGED), ("1", None), "K")),
        ],
    )
    chunks = [c for c in output if isinstance(c, f2.Chunk)]
    assert chunks
    before = dict(L.VALIDATION_STATS)
    for chunk in chunks:
        L.validate_chunk(chunk)
    assert L.VALIDATION_STATS["fast"] - before["fast"] == len(chunks)
    assert L.VALIDATION_STATS["exact"] == before["exact"]


def test_mutated_chunks_get_the_exact_validators_outcome(chunks):
    rng = random.Random(20261108)
    hits = {name: 0 for name in MUTATIONS}  # rejections the exact validator made, per class
    fast_accepts = 0
    for _ in range(N):
        name = rng.choice(list(MUTATIONS))
        mutant = MUTATIONS[name](rng, rng.choice(chunks))
        if mutant is None:
            continue
        exact = _outcome(L._validate_chunk_rows, mutant)
        before = L.VALIDATION_STATS["fast"]
        assert _outcome(L.validate_chunk, mutant) == exact, name
        fast_accepts += L.VALIDATION_STATS["fast"] > before
        hits[name] += exact[0] == "err"
    assert all(hits.values()), hits  # every mutation class produced real rejections
    assert fast_accepts  # and valid mutants still took the fast path


def test_the_fast_path_attempts_no_imports(chunks, monkeypatch):
    """Every compute argument must be a typed Arrow value. A plain Python scalar makes
    pyarrow infer its type, which imports `decimal` and tries `dateutil.relativedelta` on
    every call (~40 us when dateutil is absent, as in Flights). So on valid chunks the
    fast path must be taken and must attempt no import at all."""
    import builtins

    valid = [c for c in chunks if not any(r["op"] == "truncate" for r in c.table.to_pylist())]
    for chunk in valid[:1]:
        L._fast_rows(chunk)  # warm any one-time lazy imports outside the measured window
    attempted = []
    real = builtins.__import__

    def recording(name, *args, **kwargs):
        attempted.append(name)
        return real(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", recording)
    before = L.VALIDATION_STATS["fast"]
    for chunk in valid:
        assert L._fast_rows(chunk) is not None
        L.validate_chunk(chunk)
    monkeypatch.undo()
    assert L.VALIDATION_STATS["fast"] - before == len(valid)
    assert attempted == [], sorted(set(attempted))


def test_an_exception_inside_the_fast_path_falls_back_and_a_stop_propagates(chunks, monkeypatch):
    chunk = next(c for c in chunks if not any(r["op"] == "truncate" for r in c.table.to_pylist()))
    expected = _outcome(L._validate_chunk_rows, chunk)

    def broken(_):
        raise pa.ArrowInvalid("boom")

    monkeypatch.setattr(L, "_fast_rows", broken)
    before = L.VALIDATION_STATS["exact"]
    assert _outcome(L.validate_chunk, chunk) == expected
    assert L.VALIDATION_STATS["exact"] == before + 1

    def stopped(_):
        raise KeyboardInterrupt

    monkeypatch.setattr(L, "_fast_rows", stopped)
    with pytest.raises(KeyboardInterrupt):
        L.validate_chunk(chunk)
