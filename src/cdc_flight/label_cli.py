"""`cdc-flight-label`: the owner's source label (source_label.py), run outside a Flight
(no Flight command imports this module) with the owner's token.

    cdc-flight-label set --label NAME [--description TEXT]
    cdc-flight-label show

Environment: CDC_SOURCE_ID, CDC_DATABASE (the change-log database, default
cdc_internal) and MOTHERDUCK_TOKEN. Ingest republishes the label to cdc_ops.sources on
its next run, withheld if it matches the source's connection. The mirror suggestion is
only printed, for setup (CDC_MIRROR_DATABASE); nothing applies it."""

import argparse
import json
import os

import duckdb

from cdc_flight import source_label
from cdc_flight.config import identifier


def main(argv=None):
    parser = argparse.ArgumentParser(prog="cdc-flight-label")
    parser.add_argument("action", choices=["set", "show"])
    parser.add_argument("--label", help="set: the display name")
    parser.add_argument("--description", help="set: an optional description or contact")
    args = parser.parse_args(argv)
    if args.action == "set" and args.label is None:
        raise SystemExit("cdc-flight-label set needs --label")
    if args.action == "show" and (args.label is not None or args.description is not None):
        raise SystemExit("cdc-flight-label show takes no --label or --description")
    try:
        source_id = identifier(os.environ.get("CDC_SOURCE_ID", ""))
        database = identifier(os.getenv("CDC_DATABASE") or "cdc_internal")
        if args.action == "set":
            source_label.validate(args.label, args.description)
    except ValueError as exc:  # LabelError messages never echo the value
        raise SystemExit(f"cdc-flight-label: {exc}") from None
    if not (os.getenv("MOTHERDUCK_TOKEN") or os.getenv("motherduck_token")):
        raise SystemExit("MOTHERDUCK_TOKEN is required; browser login is disabled")
    with duckdb.connect(f"md:{database}?attach_mode=single") as con:
        print(json.dumps(run(con, database, source_id, args.label, args.description)))


def run(con, database, source_id, label=None, description=None):
    """The command on an open connection (tests use a local database)."""
    if label is not None:
        source_label.set_label(con, database, source_id, label, description)
    row = source_label.current(con, database, source_id)
    label, description, revision = row or (None, None, None)
    out = dict(source=source_id, label=label, description=description, revision=revision)
    out["published"] = "by ingest on its next run (cdc_ops.sources), unless withheld"
    if label is not None:
        out["mirror_suggestion_for_setup"] = source_label.mirror_suggestion(label, database)
    return out
