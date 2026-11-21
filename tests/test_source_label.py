"""The owner-set source label (source_label.py, label_cli.py, cdc_ops.sources).

- Validation refuses anything connection-like, and its messages never echo the value.
- Ingest withholds a label that matches the effective connection values (password,
  every listed host and its DNS labels, whole-token user and dbname), as libpq resolved
  them, and fails closed when it can't read them.
- A relabel is the owner CLI's write only: no Flight config or rebuild. The next ingest
  report republishes it; an unchanged label costs one read and no write.
- Unset (or withheld), the Dive shows source_id. The mirror suggestion is never applied.
"""

import ast
import pathlib
from types import SimpleNamespace

import duckdb
import psycopg2
import pytest
from test_ingest_batching import small_transactions
from test_ingest_pipeline import prepared
from test_phase1 import pg_phase1  # noqa: F401 (fixture)

from cdc_flight import label_cli, ops_ingest, source_label
from cdc_flight.config import ApplyConfig
from cdc_flight.ingest import run_once
from cdc_flight.ops import Ops
from cdc_flight.source_label import (
    WITHHELD_MATCH,
    WITHHELD_UNREADABLE,
    Guard,
    LabelError,
    mirror_suggestion,
    validate,
)

SRC = pathlib.Path(__file__).resolve().parents[1] / "src" / "cdc_flight"

# What the Dive shows per source (handed to the Dive stream): the label, or source_id
# when unset or withheld, with the reason.
DISPLAY = (
    "SELECT source_id,coalesce(label,source_id) AS display_name,label IS NULL AS label_missing,"
    "label_withheld,description,owner_user FROM cdc_ops.main.sources ORDER BY source_id"
)


# --- validation ---------------------------------------------------------


@pytest.mark.parametrize(
    "label,description",
    [
        ("orders-prod", "RDS us-east-1, data-platform"),
        ("Orders 2", None),
        ("a", ""),
        ("x" * 40, "y" * 120),
        ("billing_v2.1", "team billing; pager #none".replace("#", "")),
    ],
)
def test_accepted(label, description):
    out = validate(f"  {label} ", description)
    assert out[0] == label


@pytest.mark.parametrize(
    "label,description",
    [
        ("", None),
        ("x" * 41, None),
        ("-orders", None),
        ("orders/prod", None),
        ("orders@prod", None),
        ("ok", "y" * 121),
        ("ok", "contact: ops"),
        ("ok", "a=b"),
        ("ok", "it's"),
        ("ok", "path\\x"),
        ("ok", "tab\there"),
        # connection-like, refused in either field
        ("10.0.0.12", None),
        ("db 10.1.2.3", None),
        ("ok", "primary at 192.168.1.20"),
        ("orders.example.com", None),
        ("ok", "see orders-db.internal.io today"),
        ("my-amazonaws-db", None),
        ("ok", "rds.thing"),
        ("localhost", None),
        ("ok", "password hunter2"),
        ("sslmode require", None),
        ("dbname orders", None),
        ("ok", "user bob"),
        ("ok", "host is down"),
    ],
)
def test_refused(label, description):
    with pytest.raises(LabelError) as caught:
        validate(label, description)
    message = str(caught.value)
    for value in (label, description):
        if value and len(value) > 3:
            assert value not in message


# --- the withhold guard (Addendum 1: M1, S1, S2) -------------------------------------


def fake(host="db.abc123xyz.us-east-1.rds.amazonaws.com", **kw):
    params = dict(host=kw.pop("listed", host), user=kw.get("user", "cdc_reader"))
    params["dbname"] = kw.get("dbname", "orders")
    if "hostaddr" in kw:
        params["hostaddr"] = kw["hostaddr"]
    info = SimpleNamespace(
        host=host.split(",")[0],
        password=kw.get("password", "s3cretPW"),
        user=params["user"],
        dbname=params["dbname"],
        dsn_parameters=params,
    )
    return SimpleNamespace(info=info)


@pytest.mark.parametrize(
    "text,withheld",
    [
        ("orders-prod", True),  # dbname "orders" as a whole token
        ("orders2", False),  # not a whole token
        ("billing", False),
        ("xs3cretpwx", True),  # the password, case-insensitive substring
        ("abc123xyz-main", True),  # the RDS instance id (a DNS label >= 4)
        ("east-team", False),  # "us-east-1" is one label; "east" alone is not one
        ("cdc reader", True),  # user "cdc_reader" as a token sequence
        ("cdc-orders2", False),
    ],
)
def test_guard_matches(text, withheld):
    guard = Guard.from_connection(fake())
    assert (guard.withheld(text) == WITHHELD_MATCH) is withheld
    assert (guard.withheld("fine", text) == WITHHELD_MATCH) is withheld  # description too


@pytest.mark.parametrize(
    "text", ["owner (cdc_reader)", "team,cdc_reader", "see orders;", "[orders] copy", "x/orders"]
)
def test_tokens_split_on_any_punctuation(text):
    """The description allows ( ) , ; [ ], so they separate words too."""
    assert Guard.from_connection(fake()).withheld("fine", text) == WITHHELD_MATCH


def test_concurrent_sets_cannot_share_a_revision(local):
    source_label.set_label(local, "cdc_internal", "src", "billing")
    with pytest.raises(duckdb.ConstraintException):
        local.execute(
            "INSERT INTO cdc_internal.main.source_label (source_id,label,revision,set_at) "
            "VALUES ('src','other',1,current_timestamp)"
        )


def test_short_user_and_dbname_are_not_tokens():
    guard = Guard.from_connection(fake(user="cdc", dbname="app"))
    assert guard.withheld("cdc-orders") is None
    assert guard.withheld("app team") is None


def test_every_listed_host_not_only_the_connected_one():
    con = fake(
        host="primary-node.example.net", listed="primary-node.example.net,standby7.example.net"
    )
    guard = Guard.from_connection(con)
    assert guard.withheld("standby7 replica") == WITHHELD_MATCH
    assert guard.withheld("primary-node") == WITHHELD_MATCH


def test_hostaddr_is_checked():
    guard = Guard.from_connection(fake(host="db.local.lan", hostaddr="10.20.30.40"))
    assert guard.withheld("x 10.20.30.40") == WITHHELD_MATCH


@pytest.mark.parametrize(
    "broken",
    [
        lambda c: setattr(c.info, "dsn_parameters", None),
        lambda c: setattr(c.info, "user", None) or c.info.dsn_parameters.pop("user"),
        lambda c: (setattr(c.info, "host", None), c.info.dsn_parameters.pop("host")),
        lambda c: delattr(c, "info"),
    ],
)
def test_unreadable_values_withhold(broken):
    con = fake()
    broken(con)
    guard = Guard.from_connection(con)
    assert guard.failed and guard.withheld("anything") == WITHHELD_UNREADABLE


def test_guard_never_shows_its_values():
    guard = Guard.from_connection(fake())
    assert "s3cret" not in repr(guard) and "abc123" not in repr(guard)
    assert not hasattr(guard, "__dict__")


@pytest.mark.postgres
def test_libpq_resolved_values(pg_dsn, monkeypatch):
    """S1: the values libpq resolved, including PGHOST from the environment and both
    hosts of a multi-host DSN, from a real connection."""
    options = psycopg2.extensions.parse_dsn(pg_dsn)
    host = options.pop("host")
    monkeypatch.setenv("PGHOST", host)  # the DSN itself names no host
    con = psycopg2.connect(**options)
    try:
        guard = Guard.from_connection(con)
        assert not guard.failed
        assert guard.withheld(f"x {host}") == WITHHELD_MATCH
        assert guard.withheld(f"{options['password']}") == WITHHELD_MATCH
        if len(options["dbname"]) >= 4:
            assert guard.withheld(f"{options['dbname']} copy") == WITHHELD_MATCH
    finally:
        con.close()
    monkeypatch.delenv("PGHOST")
    con = psycopg2.connect(
        **dict(
            options,
            host=f"unreachable-standby.invalid,{host}",
            port=f"{options.get('port', 5432)},{options.get('port', 5432)}",
        )
    )
    try:
        guard = Guard.from_connection(con)
        assert guard.withheld("unreachable-standby") == WITHHELD_MATCH
    finally:
        con.close()


# --- store, republish, unset ----------------------------------------------------------


@pytest.fixture
def local(tmp_path):
    con = duckdb.connect(str(tmp_path / "cdc_internal.duckdb"))
    con.execute(f"ATTACH '{tmp_path / 'cdc_ops.duckdb'}' AS cdc_ops")
    con.execute("CREATE SCHEMA src")
    con.execute(
        "CREATE TABLE src.tables (relation_id UINTEGER,schema_name VARCHAR,table_name VARCHAR,"
        "held_reason VARCHAR,held_detail VARCHAR,held_since TIMESTAMPTZ,pending_keys_v2 VARCHAR)"
    )
    yield con
    con.close()


CONFIG = SimpleNamespace(database="cdc_internal", source_id="src")


def report(con, guard, run=[0]):  # noqa: B006 (a run counter)
    run[0] += 1
    observed = dict(run_id=f"r{run[0]}", status="drained", as_of=run[0], epoch=1, checkpoint=10)
    if guard is not None:
        observed["label_guard"] = guard
    assert ops_ingest.report_ingest(con, CONFIG, observed) == "written"
    return con.execute(DISPLAY).fetchall()


def test_unset_then_set_then_relabel_without_a_rebuild(local):
    clear = Guard.from_connection(fake())
    # Unset: the Dive shows source_id.
    assert report(local, clear) == [
        ("src", "src", True, None, None, local.execute("SELECT current_user").fetchone()[0])
    ]
    # The owner's CLI writes the truth only; ingest's next report publishes it.
    out = label_cli.run(local, "cdc_internal", "src", "Billing EU", "team billing")
    assert out["revision"] == 1 and out["mirror_suggestion_for_setup"] == "billing_eu_mirror"
    assert report(local, clear)[0][1:5] == ("Billing EU", False, None, "team billing")
    # A relabel: one more CLI write, nothing else (no config, no Flight version).
    label_cli.run(
        local, "cdc_internal", "src", "Billing EU (old)".replace("(", "").replace(")", "")
    )
    rows = report(local, clear)
    assert rows[0][1] == "Billing EU old" and rows[0][4] is None
    (revision,) = local.execute("SELECT label_revision FROM cdc_ops.main.sources").fetchone()
    assert revision == 2
    history = local.execute("SELECT count(*) FROM cdc_internal.main.source_label").fetchone()
    assert history == (2,)  # append-only


def test_withheld_label_is_never_published(local):
    label_cli.run(local, "cdc_internal", "src", "orders main", "abc123xyz cluster")
    rows = report(local, Guard.from_connection(fake()))
    assert rows[0][1:5] == ("src", True, WITHHELD_MATCH, None)
    # Unreadable values: withheld too (fail closed).
    rows = report(local, Guard(failed=True))
    assert rows[0][1:4] == ("src", True, WITHHELD_UNREADABLE)
    published = local.execute("SELECT label,description FROM cdc_ops.main.sources").fetchone()
    assert published == (None, None)


def test_no_connection_keeps_the_published_row(local):
    label_cli.run(local, "cdc_internal", "src", "billing")
    report(local, Guard.from_connection(fake()))
    assert report(local, None)[0][1] == "billing"  # S4: a run without a guard writes nothing


def test_an_unchanged_label_is_one_read(local, monkeypatch):
    label_cli.run(local, "cdc_internal", "src", "billing")
    guard = Guard.from_connection(fake())
    report(local, guard)
    ops = Ops(local, "cdc_internal", "src", "cdc_ops")
    seen = []
    real = ops.execute
    monkeypatch.setattr(ops, "execute", lambda s, p=None: seen.append(s) or real(s, p))
    assert source_label.publish(ops, "cdc_internal", "src", guard) == "unchanged"
    assert len(seen) == 1
    # An edit in cdc_ops is repaired on the next run.
    local.execute("UPDATE cdc_ops.main.sources SET label='edited'")
    assert source_label.publish(ops, "cdc_internal", "src", guard) == "written"
    assert local.execute("SELECT label FROM cdc_ops.main.sources").fetchone() == ("billing",)


def test_no_truth_row_publishes_no_label(local):
    """N2: a row in cdc_ops but none in the change log (a deleted source's label)."""
    label_cli.run(local, "cdc_internal", "src", "billing")
    report(local, Guard.from_connection(fake()))
    local.execute("DELETE FROM cdc_internal.main.source_label")
    assert report(local, Guard.from_connection(fake()))[0][1:4] == ("src", True, None)


def test_a_failed_publish_never_fails_the_status_write(local, monkeypatch):
    label_cli.run(local, "cdc_internal", "src", "billing")

    def broken(*args):
        raise duckdb.IOException("boom")

    monkeypatch.setattr(source_label, "publish", broken)
    report(local, Guard.from_connection(fake()))
    assert local.execute("SELECT run_id FROM cdc_ops.main.ingest_status").fetchone() is not None


def test_cli_refuses_without_echoing(monkeypatch, capsys):
    monkeypatch.setenv("CDC_SOURCE_ID", "src")
    with pytest.raises(SystemExit) as caught:
        label_cli.main(["set", "--label", "db.secret-host.example.com"])
    assert "secret-host" not in str(caught.value) and "connection" in str(caught.value)
    with pytest.raises(SystemExit):
        label_cli.main(["show", "--label", "x"])


# --- not config, not a database name ------------------------------------------------


def test_no_flight_reads_a_label_from_config():
    """Only ingest's status writer and the owner CLI import source_label, and no
    module reads a label from the environment: a RUN override can't set it."""
    importers = set()
    for path in SRC.glob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            names = []
            if isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module] + [f"{node.module}.{a.name}" for a in node.names]
            if any(n.endswith("source_label") for n in names):
                importers.add(path.stem)
        assert "CDC_LABEL" not in path.read_text() and "SOURCE_LABEL" not in path.read_text()
    assert importers == {"ingest", "ops_ingest", "label_cli"}


@pytest.mark.parametrize(
    "label,expected",
    [
        ("Orders Prod", "orders_prod_mirror"),
        ("orders-prod", "orders_prod_mirror"),  # same as above: why it's only a suggestion
        ("2024 orders", None),
        ("cdc", "cdc_mirror"),
        ("x" * 40, "x" * 40 + "_mirror"),
    ],
)
def test_mirror_suggestion(label, expected):
    assert mirror_suggestion(label, "cdc_internal") == expected


def test_mirror_suggestion_never_names_the_change_log():
    assert mirror_suggestion("cdc internal", "cdc_internal_mirror") is None


def test_a_label_never_moves_the_mirror(local, monkeypatch):
    """After setup the mirror name is configuration only: setting a label leaves it."""
    label_cli.run(local, "cdc_internal", "src", "Billing EU")
    monkeypatch.setenv("CDC_SOURCE_ID", "src")
    monkeypatch.setenv("CDC_PG_DSN", "host=x")
    monkeypatch.delenv("CDC_MIRROR_DATABASE", raising=False)
    assert ApplyConfig.from_env().mirror_database == "src_mirror"


def test_label_cli_is_not_imported_by_any_flight_command():
    from authority_scan import flight_modules

    assert "label_cli" not in flight_modules() and "source_label" in flight_modules()


# --- the ingest run builds the guard -------------------------------------------------


@pytest.mark.postgres
def test_an_ingest_run_builds_the_guard(stores, pg_phase1, monkeypatch):  # noqa: F811
    source, config, log, _ = prepared(stores, pg_phase1, monkeypatch)
    small_transactions(source, 3)
    observed = {}
    assert run_once(config, log, observe=observed).status == "drained"
    guard = observed["label_guard"]
    assert isinstance(guard, Guard) and not guard.failed
