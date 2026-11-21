"""The format-2 arm with ApplyRun's Python fold forced (fold="python"): the SQL fold's
A/B reference on one tree. Measurement only; production has no such switch."""

import importlib.util
from pathlib import Path

from cdc_flight.apply_run import ApplyRun
from cdc_flight.config import ApplyConfig

_spec = importlib.util.spec_from_file_location("arm_f2_base", Path(__file__).with_name("arm_f2.py"))
_base = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_base)
globals().update({k: v for k, v in vars(_base).items() if not k.startswith("__")})


def apply_setup(con, config, mirror, table):  # noqa: F811 (arm_f2's, with fold="python")
    apply = ApplyRun(
        con,
        ApplyConfig(
            config["source_id"], (table,), database=config["database"], mirror_database=mirror
        ),
        fold="python",
    )
    apply.initialize()
    apply.start_from_log(table)
    return apply
