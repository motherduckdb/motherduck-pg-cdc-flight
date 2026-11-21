"""Disposable real-Postgres fixture using pgoutput's SQL peek interface.

peek never acknowledges WAL. advance() is called only after destination commit.
This proves real wire decoding; it is not the Phase 1 replication socket loop.
"""

import uuid

import psycopg2
from psycopg2 import sql

from cdc_flight.pgoutput import Decoder


class Source:
    def __init__(self, dsn):
        self._dsn = dsn
        self.con = psycopg2.connect(dsn)
        self.con.autocommit = True
        suffix = uuid.uuid4().hex[:12]
        self.schema = "cdc_p0_" + suffix
        self.slot = "cdc_p0_" + suffix
        self.publication = "cdc_p0_" + suffix
        self.created_slot = False
        self.created_publication = False
        self.created_schema = False

    def execute(self, statement, args=None):
        with self.con.cursor() as cur:
            cur.execute(statement, args)
            return cur.fetchall() if cur.description else None

    def __enter__(self):
        try:
            self.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.schema)))
            self.created_schema = True
            self.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(self.schema)))
            self.execute("CREATE TABLE items (id integer PRIMARY KEY, value text, note text)")
            self.execute("ALTER TABLE items REPLICA IDENTITY FULL")
            self.execute("CREATE TABLE heartbeat (id text PRIMARY KEY)")
            self.execute(
                sql.SQL("CREATE PUBLICATION {} FOR TABLE items, heartbeat").format(
                    sql.Identifier(self.publication)
                )
            )
            self.created_publication = True
            self.execute(
                "SELECT * FROM pg_create_logical_replication_slot(%s, 'pgoutput')", [self.slot]
            )
            self.created_slot = True
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, *exc):
        try:
            if self.created_slot:
                self.execute("SELECT pg_drop_replication_slot(%s)", [self.slot])
            if self.created_publication:
                self.execute(
                    sql.SQL("DROP PUBLICATION {}").format(sql.Identifier(self.publication))
                )
            if self.created_schema:
                self.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema)))
        finally:
            self.con.close()

    def marker(self):
        marker = uuid.uuid4().hex
        self.execute("INSERT INTO heartbeat VALUES (%s)", [marker])
        return marker

    def peek(self):
        # Fixed-size Phase 0 workloads only; Phase 1 must use a streaming connection.
        result = self.execute(
            "SELECT data FROM pg_logical_slot_peek_binary_changes("
            "%s, NULL, NULL, 'proto_version','1','publication_names',%s)",
            [self.slot, self.publication],
        )
        return [bytes(row[0]) for row in result]

    def transactions(self):
        decoder, result = Decoder(), []
        for wire in self.peek():
            decoded = decoder.feed(wire)
            if decoded:
                result.append(decoded)
        return result

    def advance(self, boundary):
        lsn = f"{boundary >> 32:X}/{boundary & 0xFFFFFFFF:X}"
        self.execute("SELECT pg_replication_slot_advance(%s,%s::pg_lsn)", [self.slot, lsn])

    def stream(self, marker, durable, *, start_lsn=0, timeout=30):
        """Bounded socket probe: durable callback must commit before feedback.

        A callback failure closes without acknowledging that transaction. Phase 1
        still needs bounded disk spooling and keepalive servicing during uploads.
        """
        import select
        import time

        from psycopg2.extras import LogicalReplicationConnection

        replication = psycopg2.connect(self._dsn, connection_factory=LogicalReplicationConnection)
        decoder = Decoder()
        deadline = time.monotonic() + timeout
        try:
            cursor = replication.cursor()
            cursor.start_replication(
                slot_name=self.slot,
                start_lsn=start_lsn,
                options={"proto_version": "1", "publication_names": self.publication},
                decode=False,
                status_interval=1,
            )
            while time.monotonic() < deadline:
                message = cursor.read_message()
                if message is None:
                    select.select(
                        [replication], [], [], min(0.1, max(0, deadline - time.monotonic()))
                    )
                    continue
                transaction = decoder.feed(message.payload)
                if transaction is None:
                    continue
                end, events = transaction
                durable(end, events)
                cursor.send_feedback(flush_lsn=end, force=True)
                if any(
                    e.relation.name == "heartbeat" and e.new and e.new[0].value == marker
                    for e in events
                ):
                    # Observe the server accepting the final feedback before disconnect.
                    while time.monotonic() < deadline:
                        confirmed = self.execute(
                            "SELECT confirmed_flush_lsn - '0/0'::pg_lsn FROM pg_replication_slots "
                            "WHERE slot_name=%s",
                            [self.slot],
                        )[0][0]
                        if int(confirmed) >= end:
                            return end
                        select.select([], [], [], 0.05)
                    break
            raise TimeoutError("replication marker/feedback not observed within probe deadline")
        finally:
            replication.close()
