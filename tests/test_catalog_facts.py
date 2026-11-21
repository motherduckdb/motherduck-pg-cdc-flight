"""preflight.catalog_facts: one catalog statement for every managed table.
Its `key` must equal preflight.catalog_key's for every kind of table,
and its facts are what ingest's per-run checks compare."""

import psycopg2
import pytest

from cdc_flight.preflight import catalog_facts, catalog_key

pytestmark = pytest.mark.postgres

SCHEMA = """
CREATE TABLE pk (id int PRIMARY KEY, v text);
CREATE TABLE composite (a int, b text, v text, PRIMARY KEY (b, a));
CREATE TABLE ident (id int PRIMARY KEY, u int NOT NULL, v text);
CREATE UNIQUE INDEX ident_u ON ident (u);
ALTER TABLE ident REPLICA IDENTITY USING INDEX ident_u;
CREATE TABLE incl (id int NOT NULL, extra int, v text);
CREATE UNIQUE INDEX incl_id ON incl (id) INCLUDE (extra);
ALTER TABLE incl REPLICA IDENTITY USING INDEX incl_id;
CREATE TABLE nokey (v text);
CREATE TABLE full_pk (id int PRIMARY KEY, v text);
ALTER TABLE full_pk REPLICA IDENTITY FULL;
CREATE TABLE root (id int PRIMARY KEY, v text) PARTITION BY RANGE (id);
CREATE TABLE root_1 PARTITION OF root FOR VALUES FROM (0) TO (10);
CREATE TABLE root_2 PARTITION OF root FOR VALUES FROM (10) TO (20);
CREATE TABLE gone (id int PRIMARY KEY);
"""
TABLES = ("pk", "composite", "ident", "incl", "nokey", "full_pk", "root", "gone")


@pytest.fixture
def source(private_pg_database):
    con = psycopg2.connect(private_pg_database)
    con.autocommit = True
    with con.cursor() as cur:
        cur.execute(SCHEMA)
        cur.execute(
            "SELECT relname,oid::bigint FROM pg_class WHERE relname = ANY(%s)", [list(TABLES)]
        )
        oids = dict(cur.fetchall())
    yield con, oids
    con.close()


def test_the_key_equals_catalog_key_for_every_kind_of_table(source):
    con, oids = source
    facts = catalog_facts(con, oids.values())
    for name in TABLES:
        assert facts[oids[name]]["key"] == catalog_key(con, oids[name]), name
    assert facts[oids["composite"]]["key"] == ["b", "a"]  # index column order
    assert facts[oids["incl"]]["key"] == ["id"]  # INCLUDE columns are not key columns
    assert facts[oids["nokey"]]["key"] is None
    assert facts[oids["pk"]]["from_pk"] and not facts[oids["ident"]]["from_pk"]
    assert facts[oids["full_pk"]]["replident"] == "f" and facts[oids["full_pk"]]["from_pk"]
    with con.cursor() as cur:
        cur.execute("DROP TABLE gone")
    after = catalog_facts(con, oids.values())
    assert oids["gone"] not in after and set(after) == set(oids.values()) - {oids["gone"]}


def test_the_facts_the_checks_compare(source):
    con, oids = source
    facts = catalog_facts(con, oids.values())
    root = facts[oids["root"]]
    assert root["relkind"] == "p"
    assert root["leaves"].startswith("2:") and len(root["leaves"]) == 2 + 32  # count:md5
    assert facts[oids["pk"]]["leaves"] is None and facts[oids["pk"]]["missing"] == []
    assert facts[oids["pk"]]["columns"] == ["id", "v"]
    pk_oid, filenode = facts[oids["full_pk"]]["pk_oid"], facts[oids["full_pk"]]["relfilenode"]
    with con.cursor() as cur:
        # A routine rebuild keeps the constraint; VACUUM FULL moves the file.
        cur.execute("REINDEX TABLE CONCURRENTLY full_pk")
        cur.execute("VACUUM FULL full_pk")
        cur.execute("ALTER TABLE pk ADD COLUMN filled int DEFAULT 7")
        cur.execute("ALTER TABLE root ADD COLUMN filled int DEFAULT 7")
        cur.execute("ALTER TABLE root DETACH PARTITION root_2")
    after = catalog_facts(con, oids.values())
    assert after[oids["full_pk"]]["pk_oid"] == pk_oid
    assert after[oids["full_pk"]]["relfilenode"] != filenode
    assert after[oids["pk"]]["missing"] == ["filled"]
    assert after[oids["pk"]]["columns"] == ["id", "v", "filled"]
    # A partitioned root's fill flag lives on its leaves only.
    assert after[oids["root"]]["missing"] == ["filled"]
    with con.cursor() as cur:
        cur.execute("ALTER TABLE full_pk DROP CONSTRAINT full_pk_pkey")
        cur.execute("ALTER TABLE full_pk ADD PRIMARY KEY (id)")
    after = catalog_facts(con, oids.values())
    assert after[oids["root"]]["leaves"].startswith("1:")
    assert after[oids["root"]]["leaves"] != root["leaves"]
    assert after[oids["full_pk"]]["pk_oid"] != pk_oid  # the same key, a new constraint
    assert after[oids["full_pk"]]["key"] == ["id"]


def test_the_start_reads_keys_only_form_has_the_same_keys(source):
    con, oids = source
    full, keys = (
        catalog_facts(con, oids.values()),
        catalog_facts(con, oids.values(), keys_only=True),
    )
    assert {o: f["key"] for o, f in full.items()} == {o: f["key"] for o, f in keys.items()}
    assert all(set(f) == {"key"} for f in keys.values())


def test_no_tables_is_no_statement():
    assert catalog_facts(None, []) == {}
