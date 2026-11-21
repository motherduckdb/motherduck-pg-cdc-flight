"""scripts/deploy_flights.py against an in-memory MotherDuck (the MD_* functions' semantics)."""

import copy
import json
import re
import sys
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import build_flight  # noqa: E402
import deploy_flights as df  # noqa: E402

EXAMPLE = ROOT / "docs/deploy_flights.example.toml"
VERSION = "0.0.1"
SHA = "0123456789abcdef0123456789abcdef01234567"
OTHER_SHA = "89abcdef0123456789abcdef0123456789abcdef"
ALL = list(df.ORDER)
# The example names rds-global.
from cdc_flight import pg_ca as _cas  # noqa: E402

CA = "rds-global" if "rds-global" in _cas.CAS else "rds-us-east-1"
CORE_AND_ACTIONS = [c for c in df.ORDER if c not in df.DESTRUCTIVE]


class FakeMD:
    """MD's interface over a dict: create/update with MD_CREATE/UPDATE_FLIGHT semantics
    (PATCH; config and secrets replaced whole), one version per change."""

    def __init__(self, org="my-org", user="owner", secrets=("cdc_pg", "cdc_flight_pkg")):
        self.org, self.user, self.secrets = org, user, set(secrets)
        self.flights_ = {}  # id -> dict(summary, versions)
        self.calls = []
        self.grants = {}

    def user_info(self):
        self.calls.append("user_info")
        return self.org, self.user

    def flights(self, owner_only):
        self.calls.append("flights")
        return [
            dict(
                flight_id=fid,
                flight_name=f["summary"]["flight_name"],
                owner_name=f["summary"]["owner_name"],
            )
            for fid, f in self.flights_.items()
            if not owner_only or f["summary"]["owner_name"] == self.user
        ]

    def state(self, fid):
        f = self.flights_[fid]
        return dict(f["summary"]), dict(f["versions"][-1])

    def grantees(self, fid):
        return self.grants.get(fid, 0)

    def secret_names(self):
        return set(self.secrets)

    def add(self, name, owner=None, **version):
        fid = str(uuid.uuid4())
        base = dict(source_code="", requirements_txt="", config={}, flight_secret_names=[],
                    instance_type="F4", max_runtime_sec=300, metadata={})  # fmt: skip
        base.update(version)
        self.flights_[fid] = dict(
            summary=dict(
                flight_name=name,
                owner_name=owner or self.user,
                current_version=1,
                schedule_cron=None,
                status="ACTIVE",
            ),  # fmt: skip
            versions=[base],
        )
        return fid

    def create(self, d):
        self.calls.append(("create", d.command))
        fid = self.add(
            d.name,
            source_code=d.source_code,
            requirements_txt=d.requirements_txt,
            config=dict(d.config),
            flight_secret_names=list(d.secrets),
            instance_type=d.instance or "F4",
            max_runtime_sec=d.max_runtime_sec,
            metadata={"chart": d.chart},
        )
        self.flights_[fid]["summary"]["schedule_cron"] = d.schedule
        self.flights_[fid]["summary"]["schedule_status"] = "ACTIVE" if d.schedule else None

    def update(self, fid, d, fields):
        self.calls.append(("update", d.command, tuple(fields)))
        f = self.flights_[fid]
        v = copy.deepcopy(f["versions"][-1])
        mapping = dict(source_code=("source_code", d.source_code),
                       requirements_txt=("requirements_txt", d.requirements_txt),
                       config=("config", dict(d.config)), secrets=("flight_secret_names", list(d.secrets)),
                       instance=("instance_type", d.instance), max_runtime_sec=("max_runtime_sec", d.max_runtime_sec),
                       chart=("metadata", {"chart": d.chart}))  # fmt: skip
        for field in fields:
            if field == "schedule" and not d.schedule:
                # As MotherDuck does (live test): '' disables, the cron stays.
                f["summary"]["schedule_status"] = "DISABLED"
            elif field == "schedule":
                f["summary"]["schedule_cron"] = d.schedule
                f["summary"]["schedule_status"] = "ACTIVE"
            else:
                key, value = mapping[field]
                v[key] = value
        if "source_code" in fields:
            v["metadata"] = {"chart": d.chart}
        f["versions"].append(v)
        f["summary"]["current_version"] += 1

    def by_name(self, name):
        return next(fid for fid, f in self.flights_.items() if f["summary"]["flight_name"] == name)

    def current(self, name):
        return self.flights_[self.by_name(name)]["versions"][-1]


def source_file(tmp_path, text=None, name="shop.toml", **replace):
    body = text if text is not None else EXAMPLE.read_text()
    body = body.replace("0123456789abcdef0123456789abcdef01234567", SHA)
    body = body.replace('pg_ca = "rds-global"', f'pg_ca = "{CA}"')
    for old, new in replace.items():
        body = body.replace(old.replace("__", " "), new)
    path = tmp_path / name
    path.write_text(body)
    return path


# The example's [secrets] uncommented: a token install (a private copy of the repository).
WITH_TOKEN = {'# [secrets]\n# package = "cdc_flight_pkg"': '[secrets]\npackage = "cdc_flight_pkg"'}


def run(md, path, *args, capsys=None):
    """main(); an --apply without --expect first plans with the same flags and approves
    that plan's digest, as a human would."""
    args = list(args)
    if "--apply" in args and "--expect" not in args:
        planned = [a for a in args if a != "--apply"]
        try:
            df.main([str(path), *planned], md=md, version=VERSION)
        except SystemExit:
            pass
        text = capsys.readouterr().out if capsys else ""
        found = re.search(r"^plan digest: (\w+)$", text, re.M)
        args += ["--expect", found.group(1) if found else "none"]
    code = df.main([str(path), *args], md=md, version=VERSION)
    out = capsys.readouterr().out if capsys else ""
    return code, out


# --- the file -------------------------------------------------------------------------------


def test_the_example_loads_with_the_documented_defaults(tmp_path):
    src = df.load_source(source_file(tmp_path))
    assert (src.org, src.source, src.pg_ca, src.dsn_env) == ("my-org", "shop", CA, "cdc_pg_DSN")
    assert src.commands() == ALL and src.destructive and not src.ingest_enabled
    assert src.flights["apply"] == df.FlightSettings("cdc-shop-apply", "*/10 * * * *", 900, "F16")
    assert src.flights["epoch"].max_runtime_sec == 600


def test_the_defaults_table_is_the_documented_one():
    """docs/flights.md's table and DEFAULTS are the same, row for row."""
    text = (ROOT / "docs/flights.md").read_text()
    section = text.split("## Deploy a source's Flights", 1)[1].split("\n## ", 1)[0]
    rows = re.findall(r"^\| `([a-z-]+)` \| (.+?) \| (\d+) \| (\S+) \|", section, re.M)
    documented = {}
    for command, schedule, runtime, size in rows:
        cron = re.fullmatch(r"`((?:\S+ ){4}\S+)`", schedule)  # a cron; `[ingest] enabled` isn't
        documented[command] = (
            cron.group(1) if cron else None,
            int(runtime),
            None if size == "default" else size,
        )
    expected = {c: (d.schedule, d.max_runtime_sec, d.instance) for c, d in df.DEFAULTS.items()}
    assert documented == expected


@pytest.mark.parametrize(
    "edit, message",
    [
        ({'source = "shop"': 'source = "Shop"'}, "identifiers"),
        ({'org = "my-org"': 'org = ""'}, "org is required"),
        ({SHA: "main"}, "40-hex"),
        ({'key = "DSN"': 'key = "URL"'}, "end in DSN"),
        ({"[ingest]": '[flights.ingest]\nschedule = "* * * * *"\n[ingest]'}, "ingest schedule"),
        ({"[ingest]": '[flights.apply]\nschedule = "every minute"\n[ingest]'}, "5-field cron"),
        ({"[ingest]": '[flights.apply]\ninstance = "F64"\n[ingest]'}, "instance"),
        ({"[ingest]": '[flights.apply]\nname = "cdc-shop-ingest"\n[ingest]'}, "share a name"),
        ({"[ingest]": "[flights.nope]\n[ingest]"}, "unknown key"),
        ({"enabled = false": "enabled = false\nextra = 1"}, "unknown key"),
        (
            {'secret = "cdc_pg"': 'secret = "cdc_pg"\npublication = "postgres://u:p@h/db"'},
            "credential",
        ),
        ({"tables = [": 'tables = ["password=hunter2", '}, "credential"),
        ({'pg_ca = "rds-global"': 'pg_ca = "rds-mars-1"'}, "unknown CA"),
        ({'tables = ["public.orders", "public.customers"]': "tables = []"}, "non-empty"),
    ],
)
def test_a_bad_file_is_refused_before_any_network_call(tmp_path, edit, message):
    body = EXAMPLE.read_text().replace("0123456789abcdef0123456789abcdef01234567", SHA)
    body = body.replace('pg_ca = "rds-global"', f'pg_ca = "{CA}"')
    edit = {k.replace("rds-global", CA): v for k, v in edit.items()}
    for old, new in edit.items():
        assert old in body, old
        body = body.replace(old, new, 1)
    path = tmp_path / "bad.toml"
    path.write_text(body)
    md = FakeMD()
    with pytest.raises(SystemExit, match=message):
        df.main([str(path)], md=md, version=VERSION)
    assert md.calls == []


# --- plan and apply ----------------------------------------------------------------------------


def test_an_empty_org_plans_ten_creates_in_dependency_order(tmp_path, capsys):
    md = FakeMD()
    code, out = run(md, source_file(tmp_path), capsys=capsys)
    assert code == 0
    summary = json.loads(out.splitlines()[-1])
    assert [s["command"] for s in summary["plan"]] == ALL
    assert {s["action"] for s in summary["plan"]} == {"create"}
    assert "ingest            cdc-shop-ingest" in out and "(after apply exist)" in out
    assert "(after backfill, compact exist)" in out  # table-action
    assert not [c for c in md.calls if isinstance(c, tuple)]  # plan only


def test_apply_creates_every_flight_owner_only_then_a_rerun_is_unchanged(tmp_path, capsys):
    md, path = FakeMD(), source_file(tmp_path)
    code, out = run(md, path, "--apply", capsys=capsys)
    summary = json.loads(out.splitlines()[-1])
    assert code == 0 and summary["result"] == "ok", summary
    assert [c[1] for c in md.calls if isinstance(c, tuple)] == ALL
    assert all(row["ok"] for row in summary["read_back"])
    ids = {c: md.by_name(f"cdc-shop-{c}") for c in ALL}
    # Ingest bakes apply's id; the action Flights bake backfill's and compact's.
    assert md.current("cdc-shop-ingest")["config"]["CDC_APPLY_FLIGHT_ID"] == ids["apply"]
    action = md.current("cdc-shop-table-action")["source_code"]
    assert ids["backfill"] in action and ids["compact"] in action
    member = md.current("cdc-shop-table-membership")["source_code"]
    assert '"dsn_env": "cdc_pg_DSN"' in member and ids["backfill"] in member
    for command in ALL:
        v = md.current(f"cdc-shop-{command}")
        assert v["source_code"].startswith("# cdc_flight Flight stub.")
        assert f"EXPECTED_SHA = {SHA!r}" in v["source_code"]
        assert v["requirements_txt"] == build_flight.requirements(SHA)
        assert v["metadata"] == {"chart": build_flight.chart(command)}
        pg = command in build_flight.PG_COMMANDS
        # The example installs anonymously: no token secret.
        assert v["flight_secret_names"] == (["cdc_pg"] if pg else [])
        assert "TOKEN" not in v["source_code"] + v["requirements_txt"]
        assert (f"PG_CA = {CA!r}" in v["source_code"]) == pg
        assert set(v["config"]) == set(df.FLIGHT_CONFIG[command])
    pre = md.current("cdc-shop-preflight")["config"]
    assert json.loads(pre["CDC_TABLES"]) == ["public.orders", "public.customers"]
    assert pre["CDC_CREATE_OBJECTS"] == "1" and pre["CDC_DSN_ENV"] == "cdc_pg_DSN"
    assert md.flights_[ids["apply"]]["summary"]["schedule_cron"] == "*/10 * * * *"
    assert md.flights_[ids["ingest"]]["summary"]["schedule_cron"] is None  # until enabled
    before = len(md.calls)
    code, out = run(md, path, "--apply", capsys=capsys)
    summary = json.loads(out.splitlines()[-1])
    assert {s["action"] for s in summary["plan"]} == {"unchanged"}
    assert not [c for c in md.calls[before:] if isinstance(c, tuple)]


def test_a_new_package_updates_only_source_requirements_and_chart(tmp_path, capsys):
    md = FakeMD()
    run(md, source_file(tmp_path), "--apply", capsys=capsys)
    path = source_file(tmp_path)
    path.write_text(path.read_text().replace(SHA, OTHER_SHA))
    code, out = run(md, path, "--apply", capsys=capsys)
    assert code == 0, out
    updates = [c for c in md.calls if isinstance(c, tuple) and c[0] == "update"]
    assert [c[1] for c in updates] == ALL
    assert {c[2] for c in updates} == {("source_code", "requirements_txt")}


def test_an_override_updates_just_that_field(tmp_path, capsys):
    md = FakeMD()
    run(md, source_file(tmp_path), "--apply", capsys=capsys)
    path = source_file(
        tmp_path, **{"[ingest]": "[flights.apply]\nmax_runtime_sec = 1200\n[ingest]"}
    )
    code, out = run(md, path, capsys=capsys)
    assert (
        "apply             cdc-shop-apply                           update: max_runtime_sec" in out
    )
    run(md, path, "--apply", capsys=capsys)
    assert [c for c in md.calls if isinstance(c, tuple) and c[0] == "update"] == [
        ("update", "apply", ("max_runtime_sec",))
    ]


def test_the_ingest_schedule_follows_enabled_and_is_cleared_only_on_request(tmp_path, capsys):
    md = FakeMD()
    run(md, source_file(tmp_path), "--apply", capsys=capsys)
    on = source_file(tmp_path, name="on.toml", **{"enabled = false": "enabled = true"})
    assert run(md, on, "--apply", capsys=capsys)[0] == 0
    ingest = md.by_name("cdc-shop-ingest")
    assert md.flights_[ingest]["summary"]["schedule_cron"] == df.INGEST_SCHEDULE
    off = source_file(tmp_path)
    code, out = run(md, off, "--apply", capsys=capsys)
    assert code == 1 and "REFUSE: ingest is scheduled" in out
    assert md.flights_[ingest]["summary"]["schedule_cron"] == df.INGEST_SCHEDULE
    code, out = run(md, off, "--disable-ingest", capsys=capsys)
    assert f"update: schedule (schedule {df.INGEST_SCHEDULE} -> none)" in out
    assert run(md, off, "--apply", "--disable-ingest", capsys=capsys)[0] == 0
    summary = md.flights_[ingest]["summary"]
    assert (summary["schedule_cron"], summary["schedule_status"]) == (
        df.INGEST_SCHEDULE,
        "DISABLED",
    )
    assert {
        s["action"] for s in json.loads(run(md, off, capsys=capsys)[1].splitlines()[-1])["plan"]
    } == {"unchanged"}
    # Enabled again: the same cron, ACTIVE again.
    assert run(md, on, "--apply", capsys=capsys)[0] == 0
    assert md.flights_[ingest]["summary"]["schedule_status"] == "ACTIVE"


def test_the_destructive_flights_are_an_explicit_opt_out_and_never_deleted(tmp_path, capsys):
    md = FakeMD()
    path = source_file(tmp_path, name="optout.toml", **{"enabled = true": "enabled = false"})
    code, out = run(md, path, "--apply", capsys=capsys)
    assert code == 0
    assert [c[1] for c in md.calls if isinstance(c, tuple)] == CORE_AND_ACTIONS
    assert "epoch             cdc-shop-epoch                           opted out" in out
    # Created earlier, then opted out: left alone.
    run(md, source_file(tmp_path), "--apply", capsys=capsys)
    before = len(md.flights_)
    code, out = run(md, path, "--apply", capsys=capsys)
    assert code == 0 and len(md.flights_) == before and "opted out" in out


def test_the_optional_read_only_role_is_table_memberships_alone(tmp_path, capsys):
    md = FakeMD(secrets=("cdc_pg", "cdc_pg_ro", "cdc_flight_pkg"))
    path = source_file(
        tmp_path, **{'secret = "cdc_pg"': 'secret = "cdc_pg"\nmembership_secret = "cdc_pg_ro"'}
    )
    assert run(md, path, "--apply", capsys=capsys)[0] == 0
    member = md.current("cdc-shop-table-membership")
    assert member["flight_secret_names"] == ["cdc_pg_ro"]
    assert '"dsn_env": "cdc_pg_ro_DSN"' in member["source_code"]
    for command in ("preflight", "ingest", "backfill", "epoch", "delete-source"):
        v = md.current(f"cdc-shop-{command}")
        assert "cdc_pg" in v["flight_secret_names"] and "cdc_pg_ro" not in v["flight_secret_names"]
        assert "cdc_pg_ro_DSN" not in v["source_code"] + json.dumps(v["config"])
    # The read-only secret must exist too.
    missing = FakeMD(secrets=("cdc_pg", "cdc_flight_pkg"))
    with pytest.raises(SystemExit, match=r"missing Flights secret\(s\) \['cdc_pg_ro'\]"):
        df.main([str(path)], md=missing, version=VERSION)


def test_a_token_install_attaches_the_token_secret_and_needs_it(tmp_path, capsys):
    """[secrets] package (a private copy of the repository): every Flight installs with
    the token and carries its secret, and the stub hides it."""
    path = source_file(tmp_path, **WITH_TOKEN)
    assert df.load_source(path).package_secret == "cdc_flight_pkg"
    md = FakeMD()
    assert run(md, path, "--apply", capsys=capsys)[0] == 0
    for command in ALL:
        v = md.current(f"cdc-shop-{command}")
        assert v["requirements_txt"] == build_flight.requirements(SHA, token="cdc_flight_pkg")
        assert "${CDC_FLIGHT_GITHUB_TOKEN}" in v["requirements_txt"]
        assert "os.environ.pop(name, None)" in v["source_code"]
        pg = command in build_flight.PG_COMMANDS
        assert sorted(v["flight_secret_names"]) == ["cdc_flight_pkg"] + (["cdc_pg"] if pg else [])
    # The token's secret must exist; an anonymous install never asks for it.
    with pytest.raises(SystemExit, match=r"missing Flights secret\(s\) \['cdc_flight_pkg'\]"):
        df.main([str(path)], md=FakeMD(secrets=("cdc_pg",)), version=VERSION)
    anonymous = FakeMD(secrets=("cdc_pg",))
    assert run(anonymous, source_file(tmp_path, name="anon.toml"), "--apply", capsys=capsys)[0] == 0


def test_dropping_the_token_moves_every_flight_to_the_anonymous_install(tmp_path, capsys):
    """Once the repository is public, removing [secrets] updates each Flight's source,
    requirements and secrets, table-membership included, and a rerun is unchanged."""
    md = FakeMD()
    token = source_file(tmp_path, name="t.toml", **WITH_TOKEN)
    assert run(md, token, "--apply", capsys=capsys)[0] == 0
    anonymous = source_file(tmp_path)
    code, out = run(md, anonymous, capsys=capsys)
    assert code == 0 and "REFUSE" not in out
    for command in ALL:
        assert (
            "update: source_code, requirements_txt, secrets"
            in out.split(f"cdc-shop-{command} ")[1].splitlines()[0]
        )
    assert run(md, anonymous, "--apply", capsys=capsys)[0] == 0
    for command in ALL:
        v = md.current(f"cdc-shop-{command}")
        assert "cdc_flight_pkg" not in v["flight_secret_names"]
        assert v["requirements_txt"] == build_flight.requirements(SHA)
    assert "update" not in run(md, anonymous, capsys=capsys)[1]


@pytest.mark.parametrize("adopt", [[], ["--adopt"]], ids=["plain", "adopt"])
def test_a_membership_flight_is_never_re_pointed_to_another_secret(tmp_path, capsys, adopt):
    """A hardened table-membership (the read-only role) is never downgraded to the
    replication secret by a run, nor upgraded: the file must name what it uses."""
    md = FakeMD(secrets=("cdc_pg", "cdc_pg_ro", "cdc_flight_pkg"))
    hardened = source_file(
        tmp_path,
        name="ro.toml",
        **{'secret = "cdc_pg"': 'secret = "cdc_pg"\nmembership_secret = "cdc_pg_ro"'},
    )
    run(md, hardened, "--apply", capsys=capsys)
    one_role = source_file(tmp_path)
    code, out = run(md, one_role, *adopt, capsys=capsys)
    assert code == 1
    assert "REFUSE: it reads the PostgreSQL secret cdc_pg_ro, the file says cdc_pg" in out
    table = [line for line in out.splitlines() if "REFUSE" in line and not line.startswith("{")]
    assert len(table) == 1 and "table-membership" in table[0]  # only table-membership
    assert md.current("cdc-shop-table-membership")["flight_secret_names"] == ["cdc_pg_ro"]
    # The other way round too.
    md2 = FakeMD(secrets=("cdc_pg", "cdc_pg_ro", "cdc_flight_pkg"))
    run(md2, one_role, "--apply", capsys=capsys)
    code, out = run(md2, hardened, *adopt, capsys=capsys)
    assert (
        code == 1
        and "REFUSE: it reads the PostgreSQL secret cdc_pg, the file says cdc_pg_ro" in out
    )


@pytest.mark.parametrize(
    "extra, message",
    [
        ('membership_secret = "cdc_pg"', "is the replication secret"),
        ('membership_secret = "cdc_flight_pkg"', "secrets.package must differ"),
        ('membership_key = "DSN"', "needs postgres.membership_secret"),
        (
            'membership_secret = "cdc_pg_ro"\nmembership_key = "URL"',
            "membership_key must end in DSN",
        ),
    ],
)
def test_bad_membership_settings_are_refused(tmp_path, extra, message):
    path = source_file(
        tmp_path, **{'secret = "cdc_pg"': f'secret = "cdc_pg"\n{extra}'}, **WITH_TOKEN
    )
    with pytest.raises(df.SourceError, match=message):
        df.load_source(path)


# --- refusals --------------------------------------------------------------------------------


def test_the_wrong_org_is_refused_before_anything_else(tmp_path):
    md = FakeMD(org="someone-else")
    with pytest.raises(SystemExit, match="signed in to the organization 'someone-else'"):
        df.main([str(source_file(tmp_path))], md=md, version=VERSION)
    assert md.calls == ["user_info"]


def test_a_missing_secret_is_refused_by_name(tmp_path):
    md = FakeMD(secrets=("cdc_flight_pkg",))
    with pytest.raises(SystemExit, match=r"missing Flights secret\(s\) \['cdc_pg'\]"):
        df.main([str(source_file(tmp_path))], md=md, version=VERSION)
    assert not [c for c in md.calls if isinstance(c, tuple)]


def test_someone_elses_flight_grantees_and_foreign_flights_are_refused(tmp_path, capsys):
    path = source_file(tmp_path)
    md = FakeMD()
    md.add("cdc-shop-compact", owner="colleague")
    code, out = run(md, path, "--apply", capsys=capsys)
    assert code == 1 and "REFUSE: a Flight of that name is someone else's" in out
    assert not [c for c in md.calls if isinstance(c, tuple)]  # nothing changed at all

    md = FakeMD()
    run(md, path, "--apply", capsys=capsys)
    md.grants[md.by_name("cdc-shop-quarantine")] = 1
    code, out = run(md, path, capsys=capsys)
    assert code == 1 and "REFUSE: it has grantees" in out

    md = FakeMD()
    md.add("cdc-shop-apply", source_code="# a hand-made Flight\n")
    code, out = run(md, path, "--apply", capsys=capsys)
    assert code == 1 and "pass --adopt" in out
    code, out = run(md, path, "--apply", "--adopt", capsys=capsys)
    assert code == 0, out
    assert md.current("cdc-shop-apply")["source_code"].startswith("# cdc_flight Flight stub.")


def test_another_sources_flights_are_not_recognised(tmp_path, capsys):
    """A file for shop2 whose names point at shop's Flights: every one is refused, so a
    typo can't move another source's Flights (CDC_SOURCE_ID or OWNER source_id)."""
    md = FakeMD()
    run(md, source_file(tmp_path), "--apply", capsys=capsys)
    other = source_file(tmp_path, **{'source = "shop"': 'source = "shop2"'})
    other.write_text(
        other.read_text() + "".join(f'\n[flights.{c}]\nname = "cdc-shop-{c}"\n' for c in ALL)
    )
    code, out = run(md, other, capsys=capsys)
    assert code == 1 and out.count("REFUSE: it belongs to source 'shop'") == len(ALL)


def test_two_of_my_flights_with_one_name_are_refused(tmp_path, capsys):
    md = FakeMD()
    md.add("cdc-shop-backfill")
    md.add("cdc-shop-backfill")
    code, out = run(md, source_file(tmp_path), "--apply", capsys=capsys)
    assert code == 1 and "REFUSE: you own two Flights of that name" in out


def test_apply_needs_the_approved_digest_and_refuses_a_changed_plan(tmp_path, capsys):
    md, path = FakeMD(), source_file(tmp_path)
    with pytest.raises(SystemExit, match="needs --expect"):
        df.main([str(path), "--apply"], md=md, version=VERSION)
    code, out = run(md, path, capsys=capsys)
    approved = re.search(r"^plan digest: (\w+)$", out, re.M).group(1)
    # The package changes after the plan was approved: same actions, different content.
    path.write_text(path.read_text().replace(SHA, OTHER_SHA))
    with pytest.raises(SystemExit, match="differs from the approved one"):
        df.main([str(path), "--apply", "--expect", approved], md=md, version=VERSION)
    assert not [c for c in md.calls if isinstance(c, tuple)]
    # A Flight appearing meanwhile changes the plan too.
    path.write_text(path.read_text().replace(OTHER_SHA, SHA))
    md.add("cdc-shop-apply", source_code="# appeared meanwhile\n")
    with pytest.raises(SystemExit, match="differs from the approved one"):
        df.main([str(path), "--apply", "--adopt", "--expect", approved], md=md, version=VERSION)


def test_adopt_never_takes_another_sources_or_commands_flight(tmp_path, capsys):
    md = FakeMD()
    run(md, source_file(tmp_path), "--apply", capsys=capsys)
    md.add("cdc-shop2-apply", source_code="# hand-made\n", config={"CDC_SOURCE_ID": "shop"})
    md.add("cdc-shop2-compact", source_code=md.current("cdc-shop-apply")["source_code"],
           config={"CDC_SOURCE_ID": "shop2"})  # fmt: skip
    md.add("cdc-shop2-quarantine", source_code=md.current("cdc-shop-quarantine")["source_code"])
    other = source_file(tmp_path, name="shop2.toml", **{'source = "shop"': 'source = "shop2"'})
    code, out = run(md, other, "--adopt", capsys=capsys)
    assert "cdc-shop2-apply                          REFUSE: it belongs to source 'shop'" in out
    assert "cdc-shop2-compact                        REFUSE: it is a apply stub" in out
    assert "cdc-shop2-quarantine                     REFUSE: it belongs to source 'shop'" in out


def test_one_listing_page_or_a_refusal(tmp_path, capsys, monkeypatch):
    class Con:
        description = [("flight_id",), ("flight_name",), ("owner_name",)]

        def __init__(self, rows):
            self.rows = rows

        def execute(self, sql, params):
            assert '"OFFSET" := 0' in sql
            return self

        def fetchall(self):
            return self.rows

    monkeypatch.setattr(df.MD, "LIMIT", 3)
    assert len(df.MD(Con([("a", "x", "o"), ("b", "y", "o")])).flights(True)) == 2
    with pytest.raises(SystemExit, match="one page of 3"):
        df.MD(Con([("a", "x", "o")] * 3)).flights(True)
    with pytest.raises(SystemExit, match="one page"):
        df.MD(Con([("a", "x", "o"), ("a", "x", "o")])).flights(True)


@pytest.mark.parametrize(
    "value",
    ["public.eyewear_inventory_snapshots", "public." + "t" * 63, "reporting.EyeballMetrics_2026"],
)
def test_ordinary_names_are_not_mistaken_for_credentials(tmp_path, value):
    body = EXAMPLE.read_text().replace("0123456789abcdef0123456789abcdef01234567", SHA)
    body = body.replace('pg_ca = "rds-global"', f'pg_ca = "{CA}"')
    body = body.replace('"public.orders"', f'"{value}"')
    path = tmp_path / "names.toml"
    path.write_text(body)
    assert value in df.load_source(path).tables


@pytest.mark.parametrize(
    "value",
    [
        "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ4In0.sig",
        "AKIAIOSFODNN7EXAMPLE",
        "x" * 64,
        "host=db password=hunter2",
    ],
)
def test_credential_shapes_are_refused(tmp_path, value):
    body = EXAMPLE.read_text().replace("0123456789abcdef0123456789abcdef01234567", SHA)
    body = body.replace('pg_ca = "rds-global"', f'pg_ca = "{CA}"')
    body = body.replace('secret = "cdc_pg"', f'secret = "cdc_pg"\nslot = "{value}"')
    path = tmp_path / "secret.toml"
    path.write_text(body)
    with pytest.raises(df.SourceError, match="credential"):
        df.load_source(path)


# --- the SQL ---------------------------------------------------------------------------------


class Recorder:
    def __init__(self):
        self.statements = []
        self.description = [("x",)]

    def execute(self, sql, params):
        self.statements.append((sql, params))
        return self

    def fetchall(self):
        return []


def test_every_statement_binds_one_value_per_placeholder(tmp_path):
    src = df.load_source(source_file(tmp_path))
    d = df.desired(src, "apply", {}, VERSION)
    con = Recorder()
    md = df.MD(con)
    md.create(d)
    md.update(str(uuid.uuid4()), d, ["source_code", "config", "schedule", "instance", "secrets"])
    md.update(str(uuid.uuid4()), d, ["max_runtime_sec"])
    for sql, params in con.statements:
        assert sql.count("?") == len(params), sql
        assert "source_code := ?" not in sql or "metadata := MAP {'chart': ?}" in sql
    create, full, small = con.statements
    assert "schedule_cron := ?" in create[0] and "instance_type := ?" in create[0]
    assert "metadata" not in small[0] and "max_runtime_sec" in small[0]
    # No timestamp columns are fetched (DuckDB needs pytz for TIMESTAMPTZ).
    assert not any("SELECT *" in sql for sql, _ in con.statements)
    # Never a grant, a run or a delete.
    for sql, _ in con.statements:
        assert not re.search(r"GRANT|RUN_FLIGHT|DELETE", sql, re.I)
