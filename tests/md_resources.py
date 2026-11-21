"""Owner-scoped allocation journal, available even after a worker is killed."""

import json
import os
from contextlib import contextmanager
from pathlib import Path

ACTIVE_REMOTE = False
OWNED_DATABASES = set()
CLEANING = False


@contextmanager
def owned_cleanup(name):
    global CLEANING
    previous = CLEANING
    CLEANING = name in OWNED_DATABASES
    try:
        yield
    finally:
        CLEANING = previous


def record_resource(kind, name, event):
    directory = os.getenv("MD_SUITE_RESOURCE_DIR")
    if not directory:
        return
    if kind == "motherduck" and event == "allocated" and ACTIVE_REMOTE:
        OWNED_DATABASES.add(name)
    # One file per process avoids interleaved worker writes. Allocation is recorded
    # before DDL because an ambiguous response may already have created the object.
    path = Path(directory) / f"{os.getpid()}.jsonl"
    with path.open("a") as out:
        out.write(json.dumps({"kind": kind, "name": name, "event": event}) + "\n")
        out.flush()
        os.fsync(out.fileno())
