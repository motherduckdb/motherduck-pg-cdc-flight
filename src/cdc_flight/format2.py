"""Format-2 protocol primitives; not selected by production entry points yet.

Wire bytes are hashed directly. Reconstruction is a test oracle, not a hot-path
integrity pass. Frame layouts and cell tags are persisted protocol constants.
"""

import hashlib
import struct
from dataclasses import dataclass, field

import pyarrow as pa

FORMAT_VERSION = 2
CELL_MAPPING_REVISION = 1
ABSENT, NULL, UNCHANGED, TEXT = range(4)
EVENT, COMMIT = 1, 2
NO_OLD, OLD_KEY, OLD_FULL = 0, 1, 2
CELL_CODES = {"absent": ABSENT, "null": NULL, "unchanged": UNCHANGED, "value": TEXT}
EVENT_HEADER = struct.Struct("!BH32sIQqIQBI32s32sQ")
COMMIT_HEADER = struct.Struct("!BH32sIQqIQ32sQ")
ORDINAL = struct.Struct("!Q")
ZERO_HASH = bytes(32)

IMAGE_TYPE = pa.struct(
    [
        pa.field("values", pa.list_(pa.field("item", pa.string())), nullable=False),
        pa.field("kinds", pa.list_(pa.field("item", pa.uint8(), nullable=False)), nullable=False),
    ]
)
FRAME_SCHEMA = pa.schema(
    [
        ("record_kind", pa.uint8()),
        ("unit_start_lsn", pa.uint64()),
        ("frame_ordinal", pa.uint64()),
        ("source_epoch", pa.uint32()),
        ("commit_lsn", pa.uint64()),
        # EVENT: original logical ordinal; COMMIT: transaction_event_count.
        ("event_ordinal", pa.uint64()),
        ("commit_ts_us", pa.int64()),
        ("txid", pa.uint32()),
        ("relation_id", pa.uint32()),
        ("schema_name", pa.string()),
        ("table_name", pa.string()),
        ("schema_version", pa.string()),
        ("key_layout_hash", pa.string()),
        ("op", pa.string()),
        ("old", IMAGE_TYPE),
        ("new", IMAGE_TYPE),
        ("old_key", IMAGE_TYPE),
        ("new_key", IMAGE_TYPE),
        ("old_tuple_kind", pa.uint8()),
        ("truncate_oids", pa.list_(pa.uint32())),
        ("truncate_options", pa.uint8()),
        ("end_lsn", pa.uint64()),
        ("end_ts_us", pa.int64()),
        ("transaction_digest", pa.string()),
        ("wire_length", pa.uint64()),
    ]
)
OUTPUT_SETTINGS = (
    "TimeZone",
    "DateStyle",
    "IntervalStyle",
    "extra_float_digits",
    "bytea_output",
    "lc_monetary",
    "xmlbinary",
    "xmloption",
    "server_encoding",
    "client_encoding",
)


def uint(value, bits, name):
    if type(value) is not int or not 0 <= value < 1 << bits:
        raise ValueError(f"{name} must be uint{bits}")
    return value


def framed(data):
    return struct.pack("!Q", len(data)) + data


def domain(name):
    return framed(("cdc-flight:format2:" + name).encode("ascii"))


@dataclass(frozen=True)
class Binding:
    source_id: str
    settings: tuple
    extensions: tuple = ()
    mapping_revision: int = CELL_MAPPING_REVISION

    def __post_init__(self):
        if not isinstance(self.source_id, str) or not self.source_id:
            raise ValueError("source binding requires a source identity")
        if type(self.settings) is not tuple or any(
            type(item) is not tuple or len(item) != 2 for item in self.settings
        ):
            raise ValueError("output settings must be immutable name/value pairs")
        if type(self.extensions) is not tuple:
            raise ValueError("extension bindings must be immutable")
        uint(self.mapping_revision, 32, "mapping_revision")
        settings = dict(self.settings)
        if len(self.settings) != len(OUTPUT_SETTINGS) or set(settings) != set(OUTPUT_SETTINGS):
            raise ValueError("the complete text-output settings binding is required")
        if any(not isinstance(v, str) for v in settings.values()):
            raise ValueError("output settings must be text")
        if settings["server_encoding"] != "UTF8" or settings["client_encoding"] != "UTF8":
            raise ValueError("format 2 requires UTF8 server and client encodings")
        if self.mapping_revision != CELL_MAPPING_REVISION:
            raise ValueError("unsupported cell mapping revision")
        if any(
            not isinstance(item, tuple)
            or len(item) != 2
            or any(not isinstance(v, str) for v in item)
            for item in self.extensions
        ):
            raise ValueError("extension bindings require name/version pairs")
        if len(dict(self.extensions)) != len(self.extensions):
            raise ValueError("duplicate extension binding")

    @property
    def digest(self):
        parts = [
            domain("binding"),
            framed(self.source_id.encode()),
            struct.pack("!I", self.mapping_revision),
        ]
        for pairs in (self.settings, self.extensions):
            parts.append(struct.pack("!Q", len(pairs)))
            for name, value in sorted(pairs):
                parts.extend((framed(name.encode()), framed(value.encode())))
        return hashlib.sha256(b"".join(parts)).digest()

    def verify(self, observed, extensions):
        if dict(self.settings) != dict(observed) or dict(self.extensions) != dict(extensions):
            raise ValueError("replication text-output binding changed before streaming")


@dataclass(frozen=True)
class UnitConfig:
    revision: int = 1
    max_commits: int = 100
    target_bytes: int = 4 * 1024 * 1024
    max_delay_ms: int = 250
    chunk_bytes: int = 10 * 1024 * 1024
    chunk_frames: int = 5000
    mapping_revision: int = CELL_MAPPING_REVISION

    def __post_init__(self):
        for name in self.__dataclass_fields__:
            uint(getattr(self, name), 64, name)
            if getattr(self, name) == 0:
                raise ValueError(f"{name} must be positive")
        if self.mapping_revision != CELL_MAPPING_REVISION:
            raise ValueError("unsupported cell mapping revision")

    def header(self, binding, epoch, anchor):
        return (
            domain("unit")
            + binding
            + struct.pack("!IQ", epoch, anchor)
            + struct.pack("!7Q", *(getattr(self, k) for k in self.__dataclass_fields__))
        )


@dataclass(frozen=True)
class Descriptor:
    oid: int
    version: str
    names: tuple
    keys: tuple
    json: str
    key_hash: bytes
    namespace: str
    table_name: str
    key_indices: tuple


@dataclass(frozen=True)
class Chunk:
    epoch: int
    unit_start_lsn: int
    first_frame: int
    count: int
    event_count: int
    member_count: int
    digest: str
    chain_digest: str
    bytes: int
    descriptors: tuple
    config_revision: int
    mapping_revision: int
    table: pa.Table = field(compare=False, repr=False)

    @property
    def id(self):
        return f"{self.epoch}:{self.unit_start_lsn}:{self.first_frame}:{self.count}"


@dataclass(frozen=True)
class UnitComplete:
    epoch: int
    unit_start_lsn: int
    end_lsn: int
    member_count: int
    event_count: int
    frame_count: int
    bytes: int
    chain_digest: str
    digest: str
    config_revision: int
    marker: bool = field(compare=False)


def image(cells):
    if cells is None:
        return None
    return {"values": [c.value for c in cells], "kinds": [CELL_CODES[c.kind] for c in cells]}


def project(cells, indices):
    if cells is None:
        return None
    values, kinds = cells["values"], cells["kinds"]
    return {
        "values": [values[i] if i >= 0 else None for i in indices],
        "kinds": [kinds[i] if i >= 0 else ABSENT for i in indices],
    }


def validate_image(value, width):
    if value is None:
        return
    if type(value) is not dict or set(value) != {"values", "kinds"}:
        raise ValueError("invalid structured cell image")
    values, kinds = value["values"], value["kinds"]
    if type(values) is not list or type(kinds) is not list or len(values) != len(kinds):
        raise ValueError("cell values/kinds length mismatch")
    if len(values) != width:
        raise ValueError("tuple/descriptor width mismatch")
    for val, kind in zip(values, kinds):
        if type(kind) is not int or kind not in (ABSENT, NULL, UNCHANGED, TEXT):
            raise ValueError("invalid cell kind")
        if (kind == TEXT and type(val) is not str) or (kind != TEXT and val is not None):
            raise ValueError("cell value does not match kind")


def key_layout(keys):
    if not isinstance(keys, tuple) or len(set(keys)) != len(keys):
        raise ValueError("key names must be a unique immutable sequence")
    if any(type(key) is not str for key in keys):
        raise ValueError("key names must be text")
    return hashlib.sha256(domain("keys") + b"".join(framed(k.encode()) for k in keys)).digest()


def validate_projection(value, key, indices):
    if value is None:
        if key is not None:
            raise ValueError("key exists without its tuple image")
        return
    if key is None:
        raise ValueError("tuple image is missing its key projection")
    for i, source in enumerate(indices):
        kind = value["kinds"][source] if source >= 0 else ABSENT
        val = value["values"][source] if source >= 0 else None
        if key["kinds"][i] != kind or key["values"][i] != val:
            raise ValueError("key projection differs from tuple image")
