"""flights/actions.json (the action Flights' machine-readable API) against the code.

Every input, action, confirmation rule and literal, decision field and status in the spec
is checked here against the Flights' own parsers and constants, so the spec can't drift.
No store and no connection: each check runs a Flight's parser or a pure function.
"""

import json
import uuid
from pathlib import Path

import pytest

from cdc_flight import action_base as base
from cdc_flight import action_flight as af
from cdc_flight import delete_source_flight as dsf
from cdc_flight import epoch_flight as ef
from cdc_flight import membership_flight as mf
from cdc_flight import quarantine_flight as qf
from cdc_flight import table_actions
from cdc_flight.action_types import parse_lsn
from cdc_flight.chain import FLIGHT_CONFIG
from cdc_flight.config import lsn_text

ROOT = Path(__file__).resolve().parents[1]
SPEC = json.loads((ROOT / "flights/actions.json").read_text())
FLIGHTS = SPEC["flights"]
FORMATS = SPEC["formats"]
REFERENCE = (ROOT / "docs/actions-api.md").read_text()

PARSERS = {
    "table-action": af.TABLE_ACTION,
    "quarantine": qf.QUARANTINE,
    "table-membership": mf.MEMBERSHIP,
    "epoch": ef.EPOCH,
    "delete-source": dsf.DELETE,
}
DECISIONS = {
    "table-action": af.DECISION_FIELDS,
    "quarantine": af.DECISION_FIELDS,
    "table-membership": mf.DECISION_FIELDS,
    "epoch": ef.DECISION_FIELDS,
    "delete-source": dsf.DECISION_FIELDS,
}
SOURCE, TABLE, EPOCH, START, END = "shop", "public.items", 3, 0x5A, 0x16B
ACTIONS = [(flight, action) for flight, f in FLIGHTS.items() for action in f["actions"]]


def literal(flight, action):
    """The spec's literal for an action, filled with this file's example state."""
    template = FLIGHTS[flight]["actions"][action].get("literal")
    if template is None:
        return None
    return template.format(
        table=TABLE,
        source=SOURCE,
        epoch=EPOCH,
        next_epoch=EPOCH + 1,
        unit_start_lsn=lsn_text(START),
        unit_end_lsn=lsn_text(END),
        to_lsn=lsn_text(parse_lsn("0/5a")),
        digest="0123456789ab",
    )


def environ(flight, action, *, dry_run, confirm=True):
    """A valid environment for the action: each required key's first valid example, and
    the confirmation its rule asks for on this run."""
    spec = FLIGHTS[flight]["actions"][action]
    env = {"CDC_ACTION": action}
    for key in spec["required"]:
        fmt = FORMATS[FLIGHTS[flight]["formats"][key]]
        env[key] = (fmt.get("valid") or fmt.get("enum"))[0]
    rule, text = spec["confirm"], literal(flight, action)
    if rule == "forced_only":
        env.update(CDC_FORCE="1", CDC_CONFIRM=text)
    elif rule == "both_runs" or (rule == "real_run_only" and not dry_run and confirm):
        env["CDC_CONFIRM"] = text
    if "CDC_TABLE" in env:
        env["CDC_TABLE"] = TABLE
    if "CDC_EXPECTED_EPOCH" in env:
        env["CDC_EXPECTED_EPOCH"] = str(EPOCH)
    if dry_run:
        env["CDC_DRY_RUN"] = "1"
    else:
        env["CDC_PREVIEW_ID"] = str(uuid.uuid4())
    return env


def parse(flight, env):
    return base.parse(env, PARSERS[flight])


# The framework --------------------------------------------------------------------


def test_the_framework_constants_match():
    assert SPEC["framework_keys"] == list(base.FRAMEWORK)
    assert SPEC["preview_minutes"] == base.PREVIEW_MINUTES
    assert SPEC["request_id_pattern"] == base.REQUEST_ID.pattern
    assert SPEC["exit_codes"] == {**base.EXIT, "failed": 1}
    assert set(SPEC["lsn_keys"]) == base.LSN_KEYS
    assert set(SPEC["int_keys"]) == base.INT_KEYS
    assert set(SPEC["confirm_rules"]) == {"none", "both_runs", "real_run_only", "forced_only"}


@pytest.mark.parametrize("flight", FLIGHTS)
def test_each_flight_declares_exactly_the_spec_keys_and_actions(flight):
    f, spec = FLIGHTS[flight], PARSERS[flight]
    assert f["keys"] == list(FLIGHT_CONFIG[flight])
    assert list(f["actions"]) == list(spec.uses)
    assert f["default_name"] == "cdc-{source}-" + flight
    assert set(f["formats"]) == set(f["keys"]) - set(base.FRAMEWORK)
    assert set(f["statuses"]) <= set(SPEC["exit_codes"])
    for action, a in f["actions"].items():
        assert a["required"] == list(spec.uses[action]), action
        assert a["optional"] == list(spec.optional.get(action, ())), action
        assert a["confirm"] in SPEC["confirm_rules"]
        assert (a["confirm"] == "none") == ("literal" not in a), action
        assert a["triggers"] in (None, "backfill", "compact")


@pytest.mark.parametrize(("flight", "action"), ACTIONS)
def test_the_decision_fields_match(flight, action):
    assert FLIGHTS[flight]["actions"][action]["decision_fields"] == list(DECISIONS[flight][action])


# Results --------------------------------------------------------------------------

OWNERS = {
    "table-action": lambda: af.Owner(SOURCE, str(uuid.uuid4()), str(uuid.uuid4())),
    "quarantine": lambda: qf.Owner(SOURCE),
    "table-membership": lambda: mf.Owner(SOURCE, str(uuid.uuid4())),
    "epoch": lambda: ef.Owner(SOURCE),
    "delete-source": lambda: dsf.Owner(SOURCE),
}
MAINS = {
    "table-action": af.main,
    "quarantine": qf.main,
    "table-membership": mf.main,
    "epoch": ef.main,
    "delete-source": dsf.main,
}


def test_the_envelope_is_the_codes():
    assert SPEC["result_envelope"] == list(base.ENVELOPE)
    for name, flight in FLIGHTS.items():
        assert flight["result_fields"][: len(base.ENVELOPE)] == list(base.ENVELOPE), name
        assert set(SPEC["would_fields"]) <= set(flight["result_extras"]), name


def test_the_envelope_fills_only_what_is_missing():
    assert base.envelope({"status": "refused", "detail": "x"}) == dict(
        status="refused",
        action=None,
        table=None,
        detail="x",
        next_steps=[],
        observed={},
        run_id=None,
    )
    kept = base.envelope({"status": "would", "confirm": "DELETE shop", "drop": ["a"]})
    assert kept["confirm"] == "DELETE shop" and kept["drop"] == ["a"]
    assert base.envelope({"status": "would"})["confirm"] is None
    assert "confirm" not in base.envelope({"status": "done"})


@pytest.mark.parametrize("flight", FLIGHTS)
def test_an_input_refusal_prints_the_envelope(flight, capsys):
    """A run refused before any connection writes no audit row: its line is the only
    record, and it has the common fields too."""
    run_id = str(uuid.uuid4())
    action = next(iter(FLIGHTS[flight]["actions"]))
    for environ, named in (
        ({"CDC_ACTION": action, "MOTHERDUCK_FLIGHT_RUN_ID": run_id}, action),
        ({"CDC_ACTION": "explode", "MOTHERDUCK_FLIGHT_RUN_ID": run_id}, None),
    ):
        assert MAINS[flight](OWNERS[flight](), environ) == SPEC["exit_codes"]["refused"]
        out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
        assert list(out) == SPEC["result_envelope"], out
        assert (out["status"], out["action"], out["run_id"]) == ("refused", named, run_id)
    MAINS[flight](OWNERS[flight](), {"CDC_ACTION": "explode"})  # outside a Flight
    assert json.loads(capsys.readouterr().out.strip().splitlines()[-1])["run_id"] is None


# Inputs ---------------------------------------------------------------------------


@pytest.mark.parametrize(("flight", "action"), ACTIONS)
def test_a_valid_dry_run_and_real_run_parse(flight, action):
    for dry_run in (True, False):
        parse(flight, environ(flight, action, dry_run=dry_run))


@pytest.mark.parametrize(("flight", "action"), ACTIONS)
def test_every_input_accepts_its_valid_examples_and_refuses_the_invalid_ones(flight, action):
    f = FLIGHTS[flight]
    a = f["actions"][action]
    for key in (*a["required"], *a["optional"]):
        if key == "CDC_CONFIRM":
            continue  # by its rule, below
        fmt = FORMATS[f["formats"][key]]
        if key == "CDC_FORCE":
            continue  # 1 needs the literal: the confirmation tests cover it
        for value in fmt.get("valid") or fmt.get("enum"):
            env = environ(flight, action, dry_run=True)
            env[key] = value
            if key == "CDC_IGNORE_DELETES" and value == "1":
                env["CDC_MODE"] = a["requires"]["CDC_IGNORE_DELETES=1"].split("=")[1]
            parse(flight, env)
        for value in fmt["invalid"]:
            env = environ(flight, action, dry_run=True)
            env[key] = value
            with pytest.raises(base.InputError, match=key):
                parse(flight, env)
    # Every declared key the action doesn't use must be blank.
    for key in set(f["keys"]) - {*a["required"], *a["optional"], *base.FRAMEWORK}:
        env = environ(flight, action, dry_run=True)
        env[key] = "1"
        with pytest.raises(base.InputError, match=f"{key} is not used by {action}"):
            parse(flight, env)


@pytest.mark.parametrize("name", ["dry_run", "preview_id", "request_id"])
def test_the_framework_formats(name):
    key = {"dry_run": "CDC_DRY_RUN", "preview_id": "CDC_PREVIEW_ID", "request_id": "CDC_REQUEST_ID"}
    for value in FORMATS[name]["valid"]:
        env = environ("table-action", "compact-now", dry_run=name != "preview_id")
        if name == "dry_run" and value == "0":
            env = environ("table-action", "compact-now", dry_run=False)
        env[key[name]] = value
        parse("table-action", env)
    for value in FORMATS[name]["invalid"]:
        env = environ("table-action", "compact-now", dry_run=name != "preview_id")
        env[key[name]] = value
        with pytest.raises(base.InputError, match=key[name]):
            parse("table-action", env)


def test_a_real_run_needs_a_preview_id_and_a_dry_run_must_not_have_one():
    env = environ("table-action", "compact-now", dry_run=False)
    del env["CDC_PREVIEW_ID"]
    with pytest.raises(base.InputError, match="CDC_PREVIEW_ID is required"):
        parse("table-action", env)
    env = environ("table-action", "compact-now", dry_run=True)
    env["CDC_PREVIEW_ID"] = str(uuid.uuid4())
    with pytest.raises(base.InputError, match="CDC_PREVIEW_ID is not used by a dry run"):
        parse("table-action", env)


@pytest.mark.parametrize(("flight", "action"), ACTIONS)
def test_the_200_character_limit_on_confirmations(flight, action):
    fmt = FLIGHTS[flight]["formats"].get("CDC_CONFIRM")
    if fmt != "text200" or FLIGHTS[flight]["actions"][action]["confirm"] == "none":
        return
    env = environ(flight, action, dry_run=True)
    env["CDC_CONFIRM"] = "x" * (FORMATS[fmt]["max_length"] + 1)
    with pytest.raises(base.InputError, match="CDC_CONFIRM is too long"):
        parse(flight, env)


# Confirmations and the preview round trip -----------------------------------------


@pytest.mark.parametrize(("flight", "action"), ACTIONS)
def test_the_dry_run_and_its_real_run_hash_alike(flight, action):
    """What the spec tells a caller to send on each run spends the dry run's preview:
    for the same decision, both runs hash the same (the check that missed accept-types)."""
    dry = parse(flight, environ(flight, action, dry_run=True))
    real = parse(flight, environ(flight, action, dry_run=False))
    decided = {name: 0 for name in DECISIONS[flight][action]}
    assert base.preview_hash(dry, decided) == base.preview_hash(real, decided)


@pytest.mark.parametrize(("flight", "action"), ACTIONS)
def test_a_both_runs_literal_is_hashed(flight, action):
    """A typed-first literal is part of the preview: the real run repeats it exactly."""
    if FLIGHTS[flight]["actions"][action]["confirm"] != "both_runs":
        return
    dry = parse(flight, environ(flight, action, dry_run=True))
    env = environ(flight, action, dry_run=False)
    env["CDC_CONFIRM"] += " "
    assert base.preview_hash(dry, {}) != base.preview_hash(parse(flight, env), {})


def test_a_forced_remove_needs_its_literal_and_nothing_else_does():
    env = environ("table-membership", "remove", dry_run=True)
    env["CDC_CONFIRM"] = "REMOVE public.other"
    with pytest.raises(base.InputError, match='exactly "REMOVE public.items"'):
        parse("table-membership", env)
    env = environ("table-membership", "remove", dry_run=True)
    del env["CDC_FORCE"]
    with pytest.raises(base.InputError, match="only for a forced remove"):
        parse("table-membership", env)


def test_the_literals_are_the_flights_own():
    """Each literal template renders what the Flight compares: LSNs in lsn_text form
    (0/5a typed means 0/5A in the literal)."""
    assert literal("quarantine", "quarantine") == af.unit_literal(TABLE, EPOCH, START, END)
    assert literal("table-membership", "remove") == mf.confirmation(TABLE)
    assert literal("epoch", "recover-slot") == ef.confirmation(SOURCE, "recover-slot", EPOCH)
    assert literal("epoch", "skip-to-lsn") == ef.confirmation(
        SOURCE, "skip-to-lsn", EPOCH, parse_lsn("0/5a")
    )
    assert literal("epoch", "skip-to-lsn").endswith(" TO 0/5A")
    assert literal("delete-source", "delete-source") == dsf.confirmation(SOURCE)
    observed = dict(unit_start_lsn=START, unit_end_lsn=END)
    text, digest = table_actions.accept_confirmation(TABLE, 2, observed, [["v", [25, -1]]])
    template = FLIGHTS["table-action"]["actions"]["accept-types"]["literal"]
    assert text == template.format(table=TABLE, digest=digest) and len(digest) == 12


# The reference ---------------------------------------------------------------------


def test_the_reference_names_every_flight_action_key_and_status():
    names = {*FLIGHTS, *SPEC["exit_codes"], *SPEC["framework_keys"]}
    for flight in FLIGHTS.values():
        names |= {*flight["actions"], *flight["keys"]}
    missing = sorted(n for n in names if f"`{n}`" not in REFERENCE)
    assert not missing, missing
