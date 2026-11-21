"""Reconcile and backfill compare each column's source type
(OID, typmod), not only its mirror type. A change that keeps every value (a varchar
widening, a wider integer) is kept as the column's current type; any other holds."""

import json

import pytest

from cdc_flight.mirror import Blocked, Column, reconcile, type_kept

VARCHAR, TEXT, BPCHAR, INT2, INT4, INT8 = 1043, 25, 1042, 21, 23, 20


def col(oid, typmod=-1, dtype=None):
    dtype = dtype or ("BIGINT" if oid in (INT2, INT4, INT8) else "VARCHAR")
    return Column("v", dtype, oid, typmod)


def descriptor(oid, typmod):
    return json.dumps(dict(identity="f", columns=[[1, "id", INT4, -1], [0, "v", oid, typmod]]))


@pytest.mark.parametrize(
    "old,new,kept",
    [
        ((VARCHAR, 14), (VARCHAR, 24), True),  # varchar(10) -> varchar(20)
        ((VARCHAR, 14), (VARCHAR, -1), True),  # -> unlimited varchar
        ((VARCHAR, 14), (TEXT, -1), True),
        ((VARCHAR, 24), (VARCHAR, 14), False),  # a narrowing truncates
        ((TEXT, -1), (VARCHAR, 24), False),  # the review's text -> varchar(20) USING upper
        ((TEXT, -1), (TEXT, -1), True),  # the same type (a same-type rewrite: a stated limit)
        ((BPCHAR, 7), (BPCHAR, 9), False),  # padding changes values
        ((BPCHAR, 7), (TEXT, -1), False),
        ((INT4, -1), (INT8, -1), True),
        ((INT2, -1), (INT4, -1), True),
        ((INT8, -1), (INT4, -1), False),
    ],
)
def test_which_source_type_changes_keep_every_value(old, new, kept):
    assert type_kept(col(*old), col(*new)) is kept


def test_reconcile_holds_a_value_changing_retype_and_keeps_a_widening():
    columns = [Column("id", "BIGINT", INT4, -1), col(TEXT)]
    with pytest.raises(Blocked, match="changed type \\(its source type"):
        reconcile(columns, descriptor(VARCHAR, 24), ("id",))
    widened = reconcile(
        [Column("id", "BIGINT", INT4, -1), col(VARCHAR, 14)], descriptor(TEXT, -1), ("id",)
    )
    assert widened[1] == col(TEXT)  # persisted: later checks compare with text
    assert reconcile(widened, descriptor(TEXT, -1), ("id",)) == widened


# The pre-deploy report and its accept (type_identity), against real PostgreSQL --------

from test_backfill import drive  # noqa: E402
from test_schema_cases import schema_source  # noqa: E402,F401

from cdc_flight import type_identity  # noqa: E402
from cdc_flight.ingest import run_once  # noqa: E402


@pytest.mark.postgres
@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_the_report_lists_a_retype_and_accept_rewrites_only_after_its_dry_run(
    stores,
    schema_source,  # noqa: F811
):
    source, ingest, log, backfill, apply = schema_source
    table = f"{source.schema}.t_type"
    assert drive(backfill, table, ingest, log).status == "published"
    pg = source.con
    assert type_identity.report(apply, pg) == {}
    # A retype before the release that compares source types (text -> varchar(20)).
    source.execute("ALTER TABLE t_type ALTER COLUMN a TYPE varchar(20)")
    found = type_identity.report(apply, pg)
    assert found == {
        table: [dict(column="a", stored=[TEXT, -1], current=[VARCHAR, 24], verdict="holds")]
    }
    would = type_identity.accept(apply, pg, table, dry_run=True)
    assert would["status"] == "would" and would["columns"] == ["a"]
    assert would["confirm"].startswith(f"ACCEPT TYPES {table} ")
    wrong = type_identity.accept(apply, pg, table, confirm=f"ACCEPT TYPES {table} 0")
    assert wrong["status"] == "refused" and type_identity.report(apply, pg) == found
    done = type_identity.accept(apply, pg, table, confirm=would["confirm"])
    assert done["status"] == "done" and type_identity.report(apply, pg) == {}
    relation_id, _ = apply.registered(table)
    (audit,) = apply.execute(
        "SELECT detail FROM {m}runs WHERE relation_id=? AND status='accept-types'", [relation_id]
    ).fetchone()
    assert audit == would["confirm"]
    # The accepted type applies: no hold at the next event.
    source.execute("INSERT INTO t_type VALUES (100,'after',100)")
    assert run_once(ingest, log).status == "drained"
    assert apply.apply_table(table, "after").status == "caught_up"
    # A later widening is reported as kept, never as a hold.
    source.execute("ALTER TABLE t_type ALTER COLUMN a TYPE varchar(40)")
    (entry,) = type_identity.report(apply, pg)[table]
    assert entry["verdict"] == "widened"
    assert type_identity.accept(apply, pg, table, dry_run=True)["status"] == "already"
