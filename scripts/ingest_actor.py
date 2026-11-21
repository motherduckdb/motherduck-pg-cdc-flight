"""Subprocess crash actor for disposable integration databases; never deploy as a Flight."""

import json
import os
import sys
from dataclasses import replace
from pathlib import Path

import duckdb

from cdc_flight.config import IngestConfig
from cdc_flight.ingest import run_once
from cdc_flight.runtime import open_log

settings = json.loads(Path(sys.argv[1]).read_text())
config = replace(IngestConfig(**settings["config"]), dsn=os.environ["CDC_TEST_PG_DSN"])
if settings["address"].startswith("md:") and not (
    os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")
):
    raise SystemExit("token required; interactive auth disabled")
con = duckdb.connect(settings["address"])
log = open_log(con, config)  # use the recorded format-2 configuration
config = log.config
stage = settings["stage"]
if stage == "mid_chunk":
    from cdc_flight.spool2 import Spool

    def dies(self):
        os._exit(77)  # while building a chunk's Arrow table, before any append

    Spool._flush = dies


def crash(point):
    if point == stage:
        os._exit(77)


run_once(replace(log.config, dsn=config.dsn), log, fault=crash)
raise SystemExit("crash hook was not reached")
