"""Second MotherDuck client process for the concurrency probes; JSON in/out only."""

import json
import os
import sys

import duckdb

from cdc_flight.store import Store, ident

args = json.load(sys.stdin)
if not os.environ.get("MOTHERDUCK_TOKEN"):
    raise SystemExit("token required; interactive auth disabled")
con = duckdb.connect(args["address"])
try:
    con.execute(f"ATTACH IF NOT EXISTS 'md:{args['mirror']}' AS {ident(args['mirror'])}")
    store = Store(con, args["database"], args["mirror"])
    try:
        if args["action"] == "takeover":
            store.acquire("apply", test_clock_offset_seconds=3600)
        elif args["action"] == "pin":
            store.pin("subprocess", 0)
        else:
            raise ValueError("unknown action")
    except Exception as exc:
        print(json.dumps({"error": type(exc).__name__}))
    else:
        print(json.dumps({"ok": True}))
    sys.stdout.flush()
finally:
    con.close()
# The probe has printed its result. Native threads can abort at interpreter teardown
# ("recursive_mutex lock failed", status -6), which would turn a completed probe into a
# failure, so skip teardown. A failure before the print still exits nonzero above.
os._exit(0)
