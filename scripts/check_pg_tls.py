"""Live check of the baked verify-full TLS against a real PostgreSQL (read only).

usage: python scripts/check_pg_tls.py <ENV_VAR_NAME> [CA name, default rds-global]

Reads the DSN from the named environment variable (never from the command line) and
never prints it. Two connections, through cdc_flight's own connection_options with
the CA baked, as a --pg-ca Flight connects:
  1. the DSN as given (its real host name): must connect; prints the negotiated TLS
     version from pg_stat_ssl for this session;
  2. the same DSN with host=<the host's IPv4 address>: must fail the certificate's
     name check (OperationalError), which proves verify-full checks the name.
Exit 0 only when both behave; prints PASS or FAIL per check, nothing else secret.
"""

import socket
import sys
from dataclasses import replace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import os  # noqa: E402

import psycopg2  # noqa: E402
from psycopg2.extensions import make_dsn, parse_dsn  # noqa: E402

from cdc_flight import pg_ca  # noqa: E402
from cdc_flight.config import IngestConfig  # noqa: E402
from cdc_flight.preflight import connect  # noqa: E402


def main():
    if len(sys.argv) not in (2, 3):
        raise SystemExit(__doc__)
    variable, ca = sys.argv[1], sys.argv[2] if len(sys.argv) == 3 else "rds-global"
    dsn = os.environ.get(variable, "")
    if not dsn:
        raise SystemExit(f"{variable} is not set")
    pg_ca.bake(ca)
    config = IngestConfig("tlscheck", "unused", "unused", ("public.unused",), dsn=dsn)
    host = parse_dsn(dsn).get("host", "")
    if not host or host.replace(".", "").isdigit():
        raise SystemExit("the DSN's host must be a host name (the check replaces it with its IP)")
    ok = True

    try:
        con = connect(config)
        try:
            with con.cursor() as cur:
                cur.execute("SELECT ssl, version FROM pg_stat_ssl WHERE pid = pg_backend_pid()")
                ssl_on, version = cur.fetchone()
        finally:
            con.close()
        good = ssl_on is True
        print(f"{'PASS' if good else 'FAIL'} host name: connected, ssl={ssl_on} version={version}")
        ok &= good
    except psycopg2.OperationalError as exc:
        print(f"FAIL host name: did not connect ({type(exc).__name__})")
        ok = False

    address = socket.getaddrinfo(host, None, socket.AF_INET)[0][4][0]
    by_ip = replace(config, dsn=make_dsn(dsn, host=address))
    try:
        connect(by_ip).close()
        print("FAIL host=IP: connected, so the name was not checked")
        ok = False
    except psycopg2.OperationalError as exc:
        named = "does not match" in str(exc) or "server certificate" in str(exc)
        print(
            f"{'PASS' if named else 'FAIL'} host=IP: refused ({'name check' if named else 'other error'})"
        )
        ok &= named
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
