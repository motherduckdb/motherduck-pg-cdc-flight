"""The owner-set source label: a display name and an optional description or contact.

- **Truth:** `<change-log database>.main.source_label`, append-only (newest revision
  wins). Only the owner's token writes it (`cdc-flight label`), so a RUN grantee's
  config can't set it: no Flight reads a label from config.
- **Display:** `cdc_ops.sources`, republished by ingest (the one writer). Ingest holds
  the source's effective connection values, so it withholds a label that matches any of
  them (password, hosts, dbname, user) and fails closed when it can't read them.
- **Cosmetic only:** nothing resolves a database name from a label. `mirror_suggestion`
  is printed for the owner to accept at setup; it is never applied.

Best effort on the ingest side: a failed read or write keeps the published row and
never changes the run's result.
"""

import logging
import re

from cdc_flight.config import identifier, quoted

logger = logging.getLogger(__name__)

LABEL_MAX, DESCRIPTION_MAX = 40, 120
LABEL_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9 ._-]*")
# Printable ASCII without @ : / = ? # \ " ' (no URL, DSN keyword or quoting characters).
DESCRIPTION_RE = re.compile(r"[ !$%&()*+,\-.0-9;<>A-Z\[\]^_`a-z{|}~]*")
IPV4_RE = re.compile(r"(?<![0-9])[0-9]{1,3}(\.[0-9]{1,3}){3}(?![0-9])")
DOTTED_RE = re.compile(r"[a-z0-9-]+(\.[a-z0-9-]+)*\.[a-z]{2,}", re.IGNORECASE)
REFUSED_SUBSTRINGS = ("amazonaws", "rds.", "localhost", "postgres://", "postgresql://")
REFUSED_WORDS = {"password", "sslmode", "dbname", "user", "host"}
# Words for the whole-token match: any run of non-alphanumerics separates them, so
# "(cdc_reader)", "x,orders_prod" and "orders_prod;" match as the bare value does.
TOKEN_SPLIT = re.compile(r"[^a-z0-9]+")
MIN_TOKEN_VALUE, MIN_DNS_LABEL = 4, 4

WITHHELD_MATCH = "matches source connection"
WITHHELD_UNREADABLE = "connection values unreadable"

TABLE = "source_label"
DDL = (
    "source_id VARCHAR NOT NULL,label VARCHAR NOT NULL,description VARCHAR,"
    "revision BIGINT NOT NULL,set_at TIMESTAMPTZ NOT NULL,set_by VARCHAR,"
    "PRIMARY KEY(source_id,revision)"
)


class LabelError(ValueError):
    """A refused label or description. Messages never echo the value."""


def _tokens(text):
    return [t for t in TOKEN_SPLIT.split(text.lower()) if t]


def validate(label, description=None):
    """(label, description) stripped and checked, or LabelError."""
    label = (label or "").strip()
    if not 1 <= len(label) <= LABEL_MAX or not LABEL_RE.fullmatch(label):
        raise LabelError(
            f"label: 1-{LABEL_MAX} characters, letters, digits, space, '.', '_' or '-', "
            "starting with a letter or digit"
        )
    if description is not None:
        description = description.strip() or None
    if description is not None and (
        len(description) > DESCRIPTION_MAX or not DESCRIPTION_RE.fullmatch(description)
    ):
        raise LabelError(
            f"description: at most {DESCRIPTION_MAX} printable characters, "
            "without @ : / = ? # \\ \" '"
        )
    for name, text in (("label", label), ("description", description)):
        if text is not None and looks_like_connection(text):
            raise LabelError(f"{name} looks like connection details")
    return label, description


def looks_like_connection(text):
    """Coarse on purpose: a display name never needs any of these."""
    lowered = text.lower()
    if IPV4_RE.search(text) or any(s in lowered for s in REFUSED_SUBSTRINGS):
        return True
    if any(DOTTED_RE.fullmatch(word) for word in text.split()):
        return True
    return bool(REFUSED_WORDS.intersection(_tokens(text)))


def mirror_suggestion(label, change_log_database):
    """`<slug>_mirror` for the owner to accept at setup as CDC_MIRROR_DATABASE, or None
    when the slug isn't a valid, distinct identifier. Never applied by anything."""
    slug = re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")
    if not slug or not slug[0].isalpha():
        return None
    name = f"{slug}_mirror"
    try:
        identifier(name)
    except ValueError:
        return None
    return None if name == change_log_database else name


class Guard:
    """The effective connection values of one PostgreSQL connection, as libpq resolved
    them (DSN, PG* environment, service file), kept only in memory: never logged,
    serialized or sent anywhere. `failed` means a value couldn't be read: withhold."""

    __slots__ = ("_secrets", "_tokens", "failed")

    def __init__(self, substrings=(), token_values=(), failed=False):
        self._secrets = tuple(s.lower() for s in substrings if s)
        self._tokens = tuple(" ".join(_tokens(v)) for v in token_values if v)
        self.failed = failed

    def __repr__(self):
        return f"Guard(failed={self.failed})"

    @classmethod
    def from_connection(cls, con):
        """From a connected psycopg2 connection; never raises (fails closed)."""
        try:
            info = con.info
            params = info.dsn_parameters  # every listed host, not only the connected one
            hosts = set()
            for key in ("host", "hostaddr"):
                hosts.update(h.strip() for h in (params.get(key) or "").split(","))
            hosts.add(info.host or "")
            hosts.discard("")
            if not hosts:
                return cls(failed=True)
            substrings = [info.password or ""]
            for host in hosts:
                substrings.append(host)
                if not IPV4_RE.fullmatch(host) and not host.startswith("/"):
                    substrings += [p for p in host.split(".") if len(p) >= MIN_DNS_LABEL]
            values = [info.user or params.get("user"), info.dbname or params.get("dbname")]
            if not all(values):
                return cls(failed=True)
            tokens = [v for v in values if len(v) >= MIN_TOKEN_VALUE]
            return cls(substrings, tokens)
        except Exception as exc:
            logger.warning("label guard unavailable (%s)", type(exc).__name__)
            return cls(failed=True)

    def withheld(self, *texts):
        """None when every text is clear, else the reason to publish no label."""
        if self.failed:
            return WITHHELD_UNREADABLE
        for text in texts:
            if not text:
                continue
            lowered = text.lower()
            if any(s in lowered for s in self._secrets):
                return WITHHELD_MATCH
            padded = f" {' '.join(_tokens(text))} "
            if any(f" {t} " in padded for t in self._tokens):
                return WITHHELD_MATCH
        return None


def _table(database):
    return f"{quoted(database)}.main.{TABLE}"


def set_label(con, database, source_id, label, description=None):
    """The owner CLI's write: validate, then append the next revision. Returns it."""
    identifier(source_id)
    label, description = validate(label, description)
    con.execute(f"CREATE TABLE IF NOT EXISTS {_table(database)} ({DDL})")
    con.execute("BEGIN")
    try:
        (revision,) = con.execute(
            f"SELECT coalesce(max(revision),0)+1 FROM {_table(database)} WHERE source_id=?",
            [source_id],
        ).fetchone()
        con.execute(
            f"INSERT INTO {_table(database)} (source_id,label,description,revision,set_at,set_by) "
            "VALUES (?,?,?,?,current_timestamp,current_user)",
            [source_id, label, description, revision],
        )
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise
    return revision


def current(con, database, source_id):
    """(label, description, revision) of the newest revision, or None."""
    if not _has_table(con, database):
        return None
    return con.execute(
        f"SELECT label,description,revision FROM {_table(database)} WHERE source_id=? "
        "ORDER BY revision DESC LIMIT 1",
        [source_id],
    ).fetchone()


def _has_table(con, database):
    """From the client-side catalog: no statement reaches MotherDuck."""
    return bool(
        con.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE database_name=? AND schema_name='main' "
            "AND table_name=?",
            [database, TABLE],
        ).fetchone()[0]
    )


SHOWN = ("label", "description", "label_revision", "label_withheld", "owner_user")


def publish(ops, database, source_id, guard):
    """Ingest's republish into `ops` (an attached ops.Ops): one read, and a write only
    when the published row differs (first run, a relabel, a withhold change, a new
    owner or an edit in cdc_ops). `guard` None (no connection this run): nothing."""
    if guard is None:
        return "skipped"
    truth = (
        f"(SELECT {{'label':label,'description':description,'revision':revision}} "
        f"FROM {_table(database)} WHERE source_id=$1 ORDER BY revision DESC LIMIT 1)"
        if _has_table(ops.con, database)
        else "NULL"
    )
    shown = (
        f"(SELECT {{{','.join(f'{c!r}:{c}' for c in SHOWN)}}} FROM {{o}}sources WHERE source_id=$1)"
    )
    row, have, user = ops.execute(f"SELECT {truth},{shown},current_user", [source_id]).fetchone()
    label = description = revision = withheld = None
    if row is not None:
        revision = row["revision"]
        withheld = guard.withheld(row["label"], row["description"])
        if withheld is None:
            label, description = row["label"], row["description"]
    want = dict(
        label=label,
        description=description,
        label_revision=revision,
        label_withheld=withheld,
        owner_user=user,
    )
    if have is not None and all(have[c] == want[c] for c in SHOWN):
        return "unchanged"
    columns = ("source_id", *SHOWN)
    ops.execute(
        f"INSERT INTO {{o}}sources ({','.join(columns)},written_at) VALUES "
        f"({','.join('?' * len(columns))},current_timestamp) ON CONFLICT(source_id) DO UPDATE SET "
        + ",".join(f"{c}=excluded.{c}" for c in (*SHOWN, "written_at")),
        [source_id] + [want[c] for c in SHOWN],
    )
    return "written"
