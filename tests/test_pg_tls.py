"""verify-full TLS for PostgreSQL, baked at build time (pg_ca.py, build_flight --pg-ca).

No network except the local test PostgreSQL, which has no TLS: a baked connection
there must fail closed, never fall back.
"""

import hashlib
import os
import re
import ssl
import stat
import subprocess
import sys
import textwrap
from dataclasses import replace
from pathlib import Path
from uuid import UUID

import duckdb
import psycopg2
import pytest
from test_format2_protocol import binding

from cdc_flight import ops_apply, ops_ingest, pg_ca, preflight
from cdc_flight.chain import FLIGHT_CONFIG
from cdc_flight.config import IngestConfig
from cdc_flight.format2 import UnitConfig
from cdc_flight.replication import ReplicationStream
from cdc_flight.runtime import open_log

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import build_flight  # noqa: E402
from flight_stub import SHA, VERSION, assert_stub_ok, stub_env, stub_for  # noqa: E402

CA = "rds-us-east-1"
PASSWORD = "tls-test-password-marker"
DSN = f"host=db.example.com port=5432 dbname=shop user=cdc password={PASSWORD}"


@pytest.fixture(autouse=True)
def fresh():
    pg_ca._reset()
    yield
    pg_ca._reset()


def config(dsn=DSN, allow_local=False):
    return IngestConfig("shop", "slot", "pub", ("public.t",), dsn=dsn, allow_local=allow_local)


# The pin -----------------------------------------------------------------------------------


def test_the_bundle_matches_its_pin_and_holds_three_roots():
    pem, pin = pg_ca.CAS[CA]
    assert len(pem.encode()) == 4576
    assert hashlib.sha256(pem.encode()).hexdigest() == pin == pg_ca.RDS_US_EAST_1_SHA256
    blocks = re.findall(
        r"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----\n", pem, flags=re.S
    )
    assert len(blocks) == 3
    for block in blocks:
        assert ssl.PEM_cert_to_DER_cert(block)


def test_a_tampered_bundle_fails_before_anything_connects(monkeypatch):
    pem, pin = pg_ca.CAS[CA]
    monkeypatch.setitem(pg_ca.CAS, CA, (pem.replace("MII", "MIJ", 1), pin))
    with pytest.raises(pg_ca.CAError, match="pinned sha256"):
        pg_ca.bake(CA)
    monkeypatch.setitem(pg_ca.CAS, CA, (pem, pin))
    pg_ca.bake(CA)
    monkeypatch.setitem(pg_ca.CAS, CA, (pem + "x", pin))  # changed after the bake
    with pytest.raises(pg_ca.CAError, match="pinned sha256"):
        pg_ca.ca_file()


GLOBAL = "rds-global"


def test_the_global_bundle_matches_its_pin_and_holds_every_regions_roots():
    pem = pg_ca._checked(GLOBAL)
    assert hashlib.sha256(pem.encode()).hexdigest() == pg_ca.RDS_GLOBAL_SHA256
    blocks = re.findall(
        r"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----\n", pem, flags=re.S
    )
    assert len(blocks) == 114 and "".join(blocks) == pem
    for block in blocks:
        assert ssl.PEM_cert_to_DER_cert(block)
    # It covers the regional bundle's roots: one name serves every RDS region.
    regional = re.findall(
        r"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----\n", pg_ca.CAS[CA][0], flags=re.S
    )
    assert set(regional) <= set(blocks)


def test_a_tampered_global_bundle_fails_closed(monkeypatch):
    from cdc_flight import rds_global_ca

    monkeypatch.setattr(rds_global_ca, "PEM", rds_global_ca.PEM.replace("MII", "MIJ", 1))
    with pytest.raises(pg_ca.CAError, match="pinned sha256"):
        pg_ca.bake(GLOBAL)
    assert pg_ca.baked() is None


def test_the_global_ca_module_missing_from_the_install_fails_closed(monkeypatch):
    """A broken install without rds_global_ca: baking the name raises CAError before
    anything connects, not an ImportError."""
    import cdc_flight

    monkeypatch.setitem(sys.modules, "cdc_flight.rds_global_ca", None)
    monkeypatch.delattr(cdc_flight, "rds_global_ca", raising=False)
    with pytest.raises(pg_ca.CAError, match="missing from this install"):
        pg_ca.bake(GLOBAL)
    assert pg_ca.baked() is None


def test_the_global_ca_file_is_the_whole_bundle():
    pg_ca.bake(GLOBAL)
    assert Path(pg_ca.ca_file()).read_bytes() == pg_ca._checked(GLOBAL).encode()
    assert pg_ca.label({"sslmode": "verify-full"}) == GLOBAL


# bake and ca_file --------------------------------------------------------------------------


def test_bake_accepts_one_known_name(monkeypatch):
    with pytest.raises(pg_ca.CAError, match="unknown"):
        pg_ca.bake("rds-eu-west-9")
    assert pg_ca.baked() is None
    pg_ca.bake(CA)
    pg_ca.bake(CA)  # the same name again: a no-op
    monkeypatch.setitem(pg_ca.CAS, "other", pg_ca.CAS[CA])
    with pytest.raises(pg_ca.CAError, match="already baked"):
        pg_ca.bake("other")
    assert pg_ca.baked() == CA


def test_no_environment_variable_bakes_or_unbakes(monkeypatch):
    for name in ("CDC_PG_CA", "PG_CA", "PGSSLMODE", "PGSSLROOTCERT", "CDC_TLS", "CDC_SSLMODE"):
        monkeypatch.setenv(name, "disable")
    assert pg_ca.baked() is None
    assert preflight.connection_options(config())["sslmode"] == "require"
    pg_ca.bake(CA)
    assert preflight.connection_options(config())["sslmode"] == "verify-full"
    assert "os.environ" not in Path(pg_ca.__file__).read_text()
    assert "getenv" not in Path(pg_ca.__file__).read_text()


def test_the_ca_file_is_private_exact_and_written_once():
    pg_ca.bake(CA)
    path = pg_ca.ca_file()
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    assert Path(path).read_bytes() == pg_ca.CAS[CA][0].encode()
    assert pg_ca.ca_file() == path


def test_the_ca_file_is_removed_at_exit():
    code = textwrap.dedent(
        f"""
        import sys; sys.path.insert(0, {str(ROOT / "src")!r})
        from cdc_flight import pg_ca
        pg_ca.bake({CA!r}); print(pg_ca.ca_file())
        """
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert not Path(out.stdout.strip()).exists()


def test_ca_file_without_a_bake_raises():
    with pytest.raises(pg_ca.CAError, match="no PostgreSQL CA"):
        pg_ca.ca_file()


# connection_options ------------------------------------------------------------------------

VARIANTS = [
    f"{DSN} sslmode=require",
    f"{DSN} sslmode=disable",
    f"{DSN} sslmode=prefer",
    DSN,
    f"{DSN} sslmode=verify-ca sslrootcert=/elsewhere/root.crt",
    f"{DSN} ssl_min_protocol_version=TLSv1",
]


@pytest.mark.parametrize("allow_local", [False, True])
@pytest.mark.parametrize("dsn", VARIANTS)
def test_baked_every_dsn_comes_out_verify_full(dsn, allow_local):
    pg_ca.bake(CA)
    options = preflight.connection_options(config(dsn, allow_local))
    assert options["sslmode"] == "verify-full"
    assert options["sslrootcert"] == pg_ca.ca_file()
    assert options["ssl_min_protocol_version"] == "TLSv1.2"
    for key, value in dict(
        host="db.example.com", port="5432", dbname="shop", user="cdc", password=PASSWORD
    ).items():
        assert options[key] == value
    assert options["connect_timeout"] == "10"


def test_baked_other_keys_survive():
    pg_ca.bake(CA)
    options = preflight.connection_options(
        config(f"{DSN} application_name=cdc-ingest connect_timeout=3")
    )
    assert options["application_name"] == "cdc-ingest" and options["connect_timeout"] == "3"


UNBAKED = [
    (f"{DSN} sslmode=require", False, dict(sslmode="require")),
    (DSN, False, dict(sslmode="require")),
    (DSN, True, dict(sslmode="disable")),
    (f"{DSN} sslmode=disable", False, dict(sslmode="disable")),
    (
        f"{DSN} sslmode=verify-full sslrootcert=/x/root.crt",
        False,
        dict(sslmode="verify-full", sslrootcert="/x/root.crt"),
    ),
]


@pytest.mark.parametrize("dsn, allow_local, expect", UNBAKED)
def test_unbaked_tls_options_are_unchanged(dsn, allow_local, expect):
    base = dict(
        host="db.example.com",
        port="5432",
        dbname="shop",
        user="cdc",
        password=PASSWORD,
        connect_timeout="10",
    )
    options = preflight.connection_options(config(dsn, allow_local))
    # Format 2 pins text output independently of transport authentication.
    assert options.pop("options").endswith("-c client_encoding=UTF8")
    assert options == {**base, **expect}


def test_the_replication_stream_gets_the_forced_options(monkeypatch):
    seen = []

    class Stop(Exception):
        pass

    def capture(conninfo):
        seen.append(conninfo.decode())
        raise Stop

    from cdc_flight import replication

    class PGconn:  # psycopg's PGconn is immutable: replace the module replication uses
        connect = staticmethod(capture)

    monkeypatch.setattr(replication, "pq", type("pq", (), dict(PGconn=PGconn)))
    pg_ca.bake(CA)
    with pytest.raises(Stop):
        ReplicationStream(preflight.connection_options(config(f"{DSN} sslmode=disable")))
    (conninfo,) = seen
    assert "sslmode=verify-full" in conninfo and f"sslrootcert={pg_ca.ca_file()}" in conninfo
    assert "replication=database" in conninfo and "ssl_min_protocol_version=TLSv1.2" in conninfo


def test_regular_and_replication_connects_get_the_forced_options(monkeypatch):
    seen = []

    class Stop(Exception):
        pass

    def capture(**kwargs):
        seen.append(kwargs)
        raise Stop

    monkeypatch.setattr(preflight.psycopg2, "connect", capture)
    pg_ca.bake(CA)
    for replication in (False, True):
        with pytest.raises(Stop):
            preflight.connect(config(f"{DSN} sslmode=require"), replication=replication)
    assert [k["sslmode"] for k in seen] == ["verify-full", "verify-full"]
    assert all(k["sslrootcert"] == pg_ca.ca_file() for k in seen)


def test_every_shipped_postgresql_connection_goes_through_connection_options():
    """Static guard: in every module a PostgreSQL command can import, the only connection
    calls are pg_connect.connect's two psycopg2.connect calls (fed by
    connection_options) and replication.py's PGconn.connect (fed by its caller's
    connection_options; ingest and bootstrap observe the stream session)."""
    from authority_scan import reach

    modules = {m for c in build_flight.PG_COMMANDS for m in reach(c)}
    calls = {}
    for name in sorted(modules):
        text = (ROOT / "src/cdc_flight" / f"{name}.py").read_text()
        found = re.findall(r"psycopg2?\.connect\(|PGconn\.connect\(|\bconnect\(\*\*", text)
        if found:
            calls[name] = len(found)
    # pg_connect: preflight's connection layer, split out so backfill can import
    # it without preflight (keyless phase 2); preflight re-exports it.
    assert calls == {"pg_connect": 2, "replication": 1}, calls
    assert "connection_factory" in (ROOT / "src/cdc_flight/pg_connect.py").read_text()
    users = [
        n
        for n in sorted(modules)
        if "ReplicationStream(" in (ROOT / "src/cdc_flight" / f"{n}.py").read_text()
        and n != "replication"
    ]
    assert users == ["bootstrap", "ingest"]
    ingest = (ROOT / "src/cdc_flight/ingest.py").read_text()
    assert "ReplicationStream(connection_options(config)" in ingest
    bootstrap = (ROOT / "src/cdc_flight/bootstrap.py").read_text()
    assert "ReplicationStream(connection_options(config)" in bootstrap


def test_check_refuses_a_baked_flight_whose_options_are_not_verify_full(monkeypatch):
    pg_ca.bake(CA)
    monkeypatch.setattr(
        preflight, "connection_options", lambda c: dict(sslmode="require", host="x")
    )
    monkeypatch.setattr(preflight, "connect", lambda *a, **k: pytest.fail("connected"))
    with pytest.raises(preflight.PreflightError, match="verify-full TLS with its baked CA"):
        preflight.check(config(allow_local=True))


def test_check_accepts_a_baked_flight_whatever_the_dsn_sslmode(monkeypatch):
    """A remote DSN saying sslmode=disable is overridden, so check doesn't refuse it;
    unbaked, it still does."""
    monkeypatch.setattr(preflight.socket, "getaddrinfo", lambda *a, **k: [object()])

    class Stop(Exception):
        pass

    def stop(*a, **k):
        raise Stop

    monkeypatch.setattr(preflight, "connect", stop)
    with pytest.raises(preflight.PreflightError, match="TLS is required"):
        preflight.check(config(f"{DSN} sslmode=disable"))
    pg_ca.bake(CA)
    with pytest.raises(Stop):
        preflight.check(config(f"{DSN} sslmode=disable"))


# The recorded state and the log line -------------------------------------------------------


def test_fields_and_the_log_line(capsys):
    assert pg_ca.fields() == dict(tls_mode=None, tls_ca=None, tls_in_use=None)
    pg_ca.bake(CA)
    preflight.connection_options(config())
    preflight.connection_options(config())
    pg_ca.note_in_use(True)
    pg_ca.note_in_use(False)  # any connection without TLS makes the run's state false
    pg_ca.note_in_use(True)
    assert pg_ca.fields() == dict(tls_mode="verify-full", tls_ca=CA, tls_in_use=False)
    out, err = capsys.readouterr()
    assert out == "" and err == f"postgres tls=verify-full ca={CA}\n"
    assert PASSWORD not in err and "db.example.com" not in err


@pytest.mark.parametrize(
    "dsn, label",
    [
        (f"{DSN} sslmode=require", None),
        (f"{DSN} sslmode=verify-full", "default"),
        (f"{DSN} sslmode=verify-ca sslrootcert=system", "system"),
        (f"{DSN} sslmode=verify-full sslrootcert=/secret/dir/root.crt", "dsn"),
    ],
)
def test_unbaked_ca_labels_never_hold_a_path(dsn, label):
    preflight.connection_options(config(dsn))
    assert pg_ca.fields()["tls_ca"] == label


# cdc_ops: run rows only --------------------------------------------------------


class Store:
    """A local DuckDB with the ops database attached, as a Flight's connection has."""

    def __init__(self, tmp_path):
        self.con = duckdb.connect(str(tmp_path / "cdc_internal.duckdb"))
        self.con.execute(f"ATTACH '{tmp_path / 'cdc_ops.duckdb'}' AS cdc_ops")
        self.con.execute("CREATE SCHEMA shop")
        self.con.execute(
            "CREATE TABLE shop.tables (relation_id UINTEGER, schema_name VARCHAR, "
            "table_name VARCHAR, held_reason VARCHAR, held_detail VARCHAR, "
            "held_since TIMESTAMPTZ,pending_keys_v2 VARCHAR)"
        )

    def rows(self, sql):
        return self.con.execute(sql).fetchall()


def ingest_config():
    return IngestConfig("shop", "slot", "pub", ("public.t",), dsn=DSN, database="cdc_internal")


def test_the_tls_columns_are_in_run_rows_only():
    for tables in (ops_ingest.TABLES, ops_ingest.PREFLIGHT, ops_apply.APPLY, ops_apply.BACKFILL):
        for name, columns in tables.items():
            has = "tls_mode" in columns
            assert has == (name in ("ingest_runs", "backfill_runs")), name
    from cdc_flight import ops

    assert "tls_mode VARCHAR,tls_ca VARCHAR,tls_in_use BOOLEAN" in ops.AUDIT["audit"]


def test_the_ingest_run_row_records_tls_and_an_old_table_gains_the_columns(tmp_path):
    store = Store(tmp_path)
    # A table as a pre-TLS build created it.
    store.con.execute(
        "CREATE TABLE cdc_ops.ingest_runs (source_id VARCHAR,run_id VARCHAR,as_of BIGINT,"
        "written_at TIMESTAMPTZ,status VARCHAR,transactions BIGINT,chunks BIGINT,"
        "duration_s DOUBLE,retained_bytes BIGINT,lag_bytes BIGINT)"
    )
    observed = dict(run_id="r1", status="drained", as_of=1, **pg_ca.fields())
    observed.update(tls_mode="verify-full", tls_ca=CA, tls_in_use=True)
    assert ops_ingest.report_ingest(store.con, ingest_config(), observed) == "written"
    assert store.rows("SELECT run_id,tls_mode,tls_ca,tls_in_use FROM cdc_ops.ingest_runs") == [
        ("r1", "verify-full", CA, True)
    ]
    status_columns = {
        r[0]
        for r in store.rows(
            "SELECT column_name FROM duckdb_columns() WHERE table_name='ingest_status'"
        )
    }
    assert not {"tls_mode", "tls_ca", "tls_in_use"} & status_columns


def test_a_later_pre_tls_run_reads_as_unknown_not_stale(tmp_path):
    """The Dive's current mode is the newest ingest_runs row. After a
    rollback, the older build's named INSERT leaves NULL: unknown, never the last
    verify-full."""
    store = Store(tmp_path)
    observed = dict(run_id="new", status="drained", as_of=1)
    observed.update(tls_mode="verify-full", tls_ca=CA, tls_in_use=True)
    ops_ingest.report_ingest(store.con, ingest_config(), observed)
    store.con.execute(  # what a pre-TLS build's report writes (named columns)
        "INSERT INTO cdc_ops.ingest_runs (source_id,run_id,as_of,written_at,status,"
        "transactions,chunks,duration_s,retained_bytes,lag_bytes) "
        "VALUES ('shop','old',2,current_timestamp+INTERVAL 1 SECOND,'drained',0,0,1,0,0)"
    )
    current = store.rows(
        "SELECT tls_mode,tls_in_use FROM cdc_ops.ingest_runs WHERE source_id='shop' "
        "ORDER BY written_at DESC LIMIT 1"
    )
    assert current == [(None, None)]


def test_backfill_and_audit_rows_record_tls(tmp_path):
    from cdc_flight import ops
    from cdc_flight.config import BackfillConfig

    store = Store(tmp_path)
    store.con.execute("ATTACH ':memory:' AS mirror")
    store.con.execute("CREATE SCHEMA mirror._cdc")
    store.con.execute(
        "CREATE TABLE mirror._cdc.generations (generation_id VARCHAR, relation_id UINTEGER, "
        "mode VARCHAR, state VARCHAR, chunks BIGINT, rows BIGINT, scan_done BOOLEAN, "
        "catchup_lsn UBIGINT, marker_lsn UBIGINT, created_at TIMESTAMPTZ, "
        "updated_at TIMESTAMPTZ, pin_lsn UBIGINT, snapshot_lsn UBIGINT)"
    )
    backfill = BackfillConfig(
        "shop", ("public.t",), database="cdc_internal", mirror_database="mirror", dsn=DSN
    )
    tls = dict(tls_mode="verify-full", tls_ca=CA, tls_in_use=False)
    assert ops_apply.report_backfill(store.con, backfill, "b1", tls=tls) == "written"
    assert ops_apply.report_backfill(store.con, backfill, "b2") == "written"  # no PostgreSQL
    assert store.rows(
        "SELECT run_id,tls_mode,tls_ca,tls_in_use FROM cdc_ops.backfill_runs ORDER BY run_id"
    ) == [("b1", "verify-full", CA, False), ("b2", None, None, None)]
    for run_id, given in (("a1", tls), ("a2", None)):
        assert ops.write_audit(
            store.con, "cdc_internal", "shop", run_id, "add", {}, True, "{}", tls=given
        )
    assert store.rows(
        "SELECT run_id,tls_mode,tls_ca,tls_in_use FROM cdc_ops.audit ORDER BY run_id"
    ) == [("a1", "verify-full", CA, False), ("a2", None, None, None)]


# build_flight ------------------------------------------------------------------------------


def test_the_postgresql_commands_are_exactly_the_six():
    assert build_flight.PG_COMMANDS == (
        "backfill",
        "delete-source",
        "epoch",
        "ingest",
        "preflight",
        "table-membership",
    )


def test_a_baked_stub_bakes_before_the_cli_imports():
    stub = build_flight.stub("ingest", SHA, VERSION, pg_ca=CA)
    assert f"PG_CA = {CA!r}\n" in stub
    assert stub.index("pg_ca.bake(PG_CA)") < stub.index("from cdc_flight.cli import main")
    assert f"PG_CA = {CA!r}" in build_flight.literals(stub)
    plain = build_flight.stub("ingest", SHA, VERSION)
    assert "PG_CA" not in plain and "pg_ca.bake" not in plain
    assert not [x for x in build_flight.literals(plain) if x.startswith("PG_CA")]


@pytest.mark.parametrize("command", ["apply", "compact", "table-action", "quarantine"])
def test_pg_ca_is_refused_for_commands_without_postgresql(command):
    owner = dict(source_id="shop") if command in build_flight.ACTION_COMMANDS else None
    if command == "table-action":
        owner.update(backfill_flight=str(UUID(int=1)), compact_flight=str(UUID(int=2)))
    with pytest.raises(ValueError, match="doesn't connect to PostgreSQL"):
        build_flight.stub(command, SHA, VERSION, owner, pg_ca=CA)


def test_an_unknown_ca_is_refused_at_build_time():
    with pytest.raises(ValueError, match="unknown CA"):
        build_flight.stub("ingest", SHA, VERSION, pg_ca="rds-mars-1")


def test_the_global_ca_is_baked_like_any_other():
    stub = build_flight.stub("ingest", SHA, VERSION, pg_ca=GLOBAL)
    assert f"PG_CA = {GLOBAL!r}\n" in stub


def test_no_flight_config_key_can_touch_tls():
    for command, keys in FLIGHT_CONFIG.items():
        for key in keys:
            assert not re.search(r"TLS|SSL|CA\b|_CA_|ROOTCERT|CERT", key), (command, key)


# Fail closed on real (local, non-TLS) PostgreSQL --------------------------------------------


@pytest.fixture
def local_dsn():
    dsn = os.getenv("CDC_TEST_PG_DSN")
    if not dsn:
        pytest.skip("set CDC_TEST_PG_DSN for real PostgreSQL probes")
    return dsn


def secret_of(dsn):
    return psycopg2.extensions.parse_dsn(dsn).get("password") or "<none>"


@pytest.mark.postgres
def test_baked_connections_to_a_non_tls_server_fail_closed(local_dsn):
    """Unbaked, the local server connects as today; baked, both connection kinds
    raise (no fallback), and the error never holds the password."""
    local = config(local_dsn, allow_local=True)
    con = preflight.connect(local)
    con.close()
    assert pg_ca.fields()["tls_in_use"] is False  # sslmode=disable, negotiated plaintext
    pg_ca._reset()
    pg_ca.bake(CA)
    for replication in (False, True):
        with pytest.raises(psycopg2.OperationalError) as caught:
            preflight.connect(local, replication=replication)
        assert secret_of(local_dsn) not in str(caught.value)
        assert local_dsn not in str(caught.value)
    assert pg_ca.fields() == dict(tls_mode="verify-full", tls_ca=CA, tls_in_use=None)


@pytest.mark.postgres
def test_prefer_records_the_negotiated_plaintext(local_dsn):
    """`prefer` can fall back silently; tls_in_use shows what was negotiated."""
    con = preflight.connect(config(f"{local_dsn} sslmode=prefer", allow_local=True))
    con.close()
    assert pg_ca.fields() == dict(tls_mode="prefer", tls_ca=None, tls_in_use=False)


RUN_FLIGHT = """
import os, re, runpy, sys
import duckdb
files = sys.argv[1]
real = duckdb.connect

class Bootstrap:
    def execute(self, *a, **k): return self
    def __enter__(self): return self
    def __exit__(self, *e): return False

class Con:
    def __init__(self, con): self._con = con
    def execute(self, statement, parameters=None):
        m = re.fullmatch(r"\\s*ATTACH IF NOT EXISTS 'md:(\\w+)' AS \\"?(\\w+)\\"?\\s*", statement)
        if m:
            statement = f"ATTACH IF NOT EXISTS '{files}/{m.group(1)}.duckdb' AS \\"{m.group(2)}\\""
        assert "'md:" not in statement, statement
        return self._con.execute(statement, parameters or [])
    def __getattr__(self, name): return getattr(self._con, name)

def connect(address=":memory:", *a, **k):
    if isinstance(address, str) and address.startswith("md:"):
        name = address[3:].split("?")[0]
        if not name:
            return Bootstrap()
        con = real(f"{files}/{name}.duckdb")
        con.execute(f"ATTACH IF NOT EXISTS '{files}/cdc_ops.duckdb' AS cdc_ops")
        return Con(con)
    return real(address, *a, **k)

duckdb.connect = connect
sys.argv = [sys.argv[2]]
runpy.run_path(sys.argv[0], run_name="__main__")
"""


@pytest.mark.postgres
def test_a_baked_ingest_stub_fails_closed_end_to_end(local_dsn, tmp_path):
    """The real stub, built with --pg-ca, run against the local non-TLS PostgreSQL:
    it refuses to connect, logs its TLS line, records the run row's tls_mode, and
    never prints the password or the DSN. MotherDuck is redirected to local files."""
    flight_py = tmp_path / "ingest.py"
    flight_py.write_text(stub_for(tmp_path, "ingest", pg_ca=CA))
    runner = tmp_path / "run_flight.py"
    runner.write_text(RUN_FLIGHT)
    files = tmp_path / "files"
    files.mkdir()
    # Ingest opens a preflighted v2 store before connecting to PostgreSQL.
    # Seed only the local metadata so TLS, rather than legacy refusal, is reached.
    with duckdb.connect(str(files / "tls_store.duckdb")) as local:
        cfg = IngestConfig("tls", "slot", "pub", ("public.t",), database="tls_store")
        log = open_log(
            local,
            cfg,
            create=True,
            binding=replace(binding(), source_id="tls"),
            unit_config=UnitConfig(),
            server_major=16,
        )
        log.register(("system", 1), 90, [])
    env = {k: v for k, v in os.environ.items() if not k.upper().startswith("MOTHERDUCK")}
    env.update(
        MOTHERDUCK_TOKEN="not-a-token-local-test",
        CDC_SOURCE_ID="tls",
        CDC_SLOT="slot",
        CDC_PUBLICATION="pub",
        CDC_TABLES='["public.t"]',
        CDC_PG_DSN=local_dsn,
        CDC_DATABASE="tls_store",
        CDC_RUNTIME_SECONDS="20",
    )
    env.update(stub_env(tmp_path))
    proc = subprocess.run(
        [sys.executable, str(runner), str(files), str(flight_py)],
        capture_output=True,
        text=True,
        env=env,
        cwd=tmp_path,
        timeout=120,
    )
    output = proc.stdout + proc.stderr
    assert proc.returncode != 0, output
    assert_stub_ok(proc.stdout)
    assert f"postgres tls=verify-full ca={CA}" in proc.stderr
    assert "OperationalError" in output
    assert secret_of(local_dsn) not in output and local_dsn not in output
    con = duckdb.connect(str(files / "cdc_ops.duckdb"), read_only=True)
    try:
        rows = con.execute("SELECT status,tls_mode,tls_ca,tls_in_use FROM ingest_runs").fetchall()
    finally:
        con.close()
    assert rows == [("failed", "verify-full", CA, None)], rows
    log = duckdb.connect(str(files / "tls_store.duckdb"), read_only=True)
    try:
        registered = log.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE schema_name='tls' AND table_name='source'"
        ).fetchone()[0]
        sources = log.execute("SELECT count(*) FROM tls.source").fetchone()[0] if registered else 0
    finally:
        log.close()
    assert sources == 1  # the preflighted registry is unchanged; no PostgreSQL connection
    with duckdb.connect(str(files / "tls_store.duckdb"), read_only=True) as local:
        assert local.execute("SELECT confirmed_lsn FROM tls.source").fetchone() == (90,)


def test_the_tls_state_reaches_every_run_row_writer():
    """Static wiring: the PostgreSQL action Flights pass pg_ca.fields() to finish (the
    audit row), the others pass nothing (NULL); backfill's report gets it; ingest notes
    the replication stream's negotiated state. Ingest's own run row is covered end to
    end above."""
    src = ROOT / "src/cdc_flight"
    for name in ("membership_flight", "epoch_flight", "delete_source_flight"):
        assert "tls=pg_ca.fields())" in (src / f"{name}.py").read_text(), name
    for name in ("action_flight", "quarantine_flight"):
        text = (src / f"{name}.py").read_text()
        assert not re.search(r"^from cdc_flight import .*\bpg_ca\b", text, re.M), name
        assert "pg_ca.fields()" not in text, name
    cli = (src / "cli.py").read_text()
    backfill = cli[cli.index("def backfill_main") :]
    assert "tls=pg_ca.fields()" in backfill
    assert "observed.update(pg_ca.fields())" in cli
    ingest = (src / "ingest.py").read_text()
    assert "pg_ca.note_in_use(replication.conn.ssl_in_use)" in ingest


@pytest.mark.postgres
def test_a_replication_protocol_connection_records_its_negotiated_tls(local_dsn):
    """Round 1 S1: connect(replication=True), as epoch.identify and preflight open it,
    feeds the tracker too; a plaintext one makes the run's tls_in_use false."""
    con = preflight.connect(
        config(f"{local_dsn} sslmode=prefer", allow_local=True), replication=True
    )
    con.close()
    assert pg_ca.fields()["tls_in_use"] is False


def test_check_refuses_a_baked_flight_with_a_foreign_root_cert(monkeypatch):
    """Round 1 S2: the defense's other half, verify-full against someone else's roots."""
    pg_ca.bake(CA)
    monkeypatch.setattr(
        preflight,
        "connection_options",
        lambda c: dict(sslmode="verify-full", sslrootcert="/tmp/other.pem", host="x"),
    )
    monkeypatch.setattr(preflight, "connect", lambda *a, **k: pytest.fail("connected"))
    with pytest.raises(preflight.PreflightError, match="verify-full TLS with its baked CA"):
        preflight.check(config(allow_local=True))
