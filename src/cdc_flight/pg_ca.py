"""PostgreSQL TLS trust roots, chosen at build time (verify-full for remote sources).

A Flight stub built with `build_flight.py --pg-ca <name>` carries a literal
`PG_CA = '<name>'` and calls `bake(PG_CA)` before the CLI is imported. Then every
PostgreSQL connection (preflight.connection_options, including ingest's replication
stream) is forced to `sslmode=verify-full` against that CA's roots: the DSN, the
secret, the environment and Flight config can't change or lower it. Config is untrusted
(anyone who can run a Flight can pass overrides, which arrive as environment variables),
so nothing here reads the environment. A stub built without --pg-ca isn't baked, and
connects as before.

The certificates aren't secret; the pin stops an edit in transit or in the repo from
silently changing the trust root. See docs/flights.md, "PostgreSQL TLS (verify-full)", for
provenance, expiry and rotation.
"""

import atexit
import hashlib
import os
import sys
import tempfile

# Amazon RDS us-east-1 bundle, verbatim (3 roots: RSA2048 G1 to 2061, RSA4096 G1 and
# ECC384 G1 to 2121). Provenance, fetched once and checked against the pin:
# https://truststore.pki.rds.amazonaws.com/us-east-1/us-east-1-bundle.pem
RDS_US_EAST_1_PEM = """\
-----BEGIN CERTIFICATE-----
MIID/zCCAuegAwIBAgIRAPVSMfFitmM5PhmbaOFoGfUwDQYJKoZIhvcNAQELBQAw
gZcxCzAJBgNVBAYTAlVTMSIwIAYDVQQKDBlBbWF6b24gV2ViIFNlcnZpY2VzLCBJ
bmMuMRMwEQYDVQQLDApBbWF6b24gUkRTMQswCQYDVQQIDAJXQTEwMC4GA1UEAwwn
QW1hem9uIFJEUyB1cy1lYXN0LTEgUm9vdCBDQSBSU0EyMDQ4IEcxMRAwDgYDVQQH
DAdTZWF0dGxlMCAXDTIxMDUyNTIyMzQ1N1oYDzIwNjEwNTI1MjMzNDU3WjCBlzEL
MAkGA1UEBhMCVVMxIjAgBgNVBAoMGUFtYXpvbiBXZWIgU2VydmljZXMsIEluYy4x
EzARBgNVBAsMCkFtYXpvbiBSRFMxCzAJBgNVBAgMAldBMTAwLgYDVQQDDCdBbWF6
b24gUkRTIHVzLWVhc3QtMSBSb290IENBIFJTQTIwNDggRzExEDAOBgNVBAcMB1Nl
YXR0bGUwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDu9H7TBeGoDzMr
dxN6H8COntJX4IR6dbyhnj5qMD4xl/IWvp50lt0VpmMd+z2PNZzx8RazeGC5IniV
5nrLg0AKWRQ2A/lGGXbUrGXCSe09brMQCxWBSIYe1WZZ1iU1IJ/6Bp4D2YEHpXrW
bPkOq5x3YPcsoitgm1Xh8ygz6vb7PsvJvPbvRMnkDg5IqEThapPjmKb8ZJWyEFEE
QRrkCIRueB1EqQtJw0fvP4PKDlCJAKBEs/y049FoOqYpT3pRy0WKqPhWve+hScMd
6obq8kxTFy1IHACjHc51nrGII5Bt76/MpTWhnJIJrCnq1/Uc3Qs8IVeb+sLaFC8K
DI69Sw6bAgMBAAGjQjBAMA8GA1UdEwEB/wQFMAMBAf8wHQYDVR0OBBYEFE7PCopt
lyOgtXX0Y1lObBUxuKaCMA4GA1UdDwEB/wQEAwIBhjANBgkqhkiG9w0BAQsFAAOC
AQEAFj+bX8gLmMNefr5jRJfHjrL3iuZCjf7YEZgn89pS4z8408mjj9z6Q5D1H7yS
jNETVV8QaJip1qyhh5gRzRaArgGAYvi2/r0zPsy+Tgf7v1KGL5Lh8NT8iCEGGXwF
g3Ir+Nl3e+9XUp0eyyzBIjHtjLBm6yy8rGk9p6OtFDQnKF5OxwbAgip42CD75r/q
p421maEDDvvRFR4D+99JZxgAYDBGqRRceUoe16qDzbMvlz0A9paCZFclxeftAxv6
QlR5rItMz/XdzpBJUpYhdzM0gCzAzdQuVO5tjJxmXhkSMcDP+8Q+Uv6FA9k2VpUV
E/O5jgpqUJJ2Hc/5rs9VkAPXeA==
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
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
0Ws=
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIICrjCCAjSgAwIBAgIRAPAlEk8VJPmEzVRRaWvTh2AwCgYIKoZIzj0EAwMwgZYx
CzAJBgNVBAYTAlVTMSIwIAYDVQQKDBlBbWF6b24gV2ViIFNlcnZpY2VzLCBJbmMu
MRMwEQYDVQQLDApBbWF6b24gUkRTMQswCQYDVQQIDAJXQTEvMC0GA1UEAwwmQW1h
em9uIFJEUyB1cy1lYXN0LTEgUm9vdCBDQSBFQ0MzODQgRzExEDAOBgNVBAcMB1Nl
YXR0bGUwIBcNMjEwNTI1MjI0MTU1WhgPMjEyMTA1MjUyMzQxNTVaMIGWMQswCQYD
VQQGEwJVUzEiMCAGA1UECgwZQW1hem9uIFdlYiBTZXJ2aWNlcywgSW5jLjETMBEG
A1UECwwKQW1hem9uIFJEUzELMAkGA1UECAwCV0ExLzAtBgNVBAMMJkFtYXpvbiBS
RFMgdXMtZWFzdC0xIFJvb3QgQ0EgRUNDMzg0IEcxMRAwDgYDVQQHDAdTZWF0dGxl
MHYwEAYHKoZIzj0CAQYFK4EEACIDYgAEx5xjrup8II4HOJw15NTnS3H5yMrQGlbj
EDA5MMGnE9DmHp5dACIxmPXPMe/99nO7wNdl7G71OYPCgEvWm0FhdvVUeTb3LVnV
BnaXt32Ek7/oxGk1T+Df03C+W0vmuJ+wo0IwQDAPBgNVHRMBAf8EBTADAQH/MB0G
A1UdDgQWBBTGXmqBWN/1tkSea4pNw0oHrjk2UDAOBgNVHQ8BAf8EBAMCAYYwCgYI
KoZIzj0EAwMDaAAwZQIxAIqqZWCSrIkZ7zsv/FygtAusW6yvlL935YAWYPVXU30m
jkMFLM+/RJ9GMvnO8jHfCgIwB+whlkcItzE9CRQ6CsMo/d5cEHDUu/QW6jSIh9BR
OGh9pTYPVkUbBiKPA7lVVhre
-----END CERTIFICATE-----
"""
RDS_US_EAST_1_SHA256 = "b1711d12bae51838581281e23b6cb97b1074016873b4dafc80ed14002462dd77"

# The Amazon RDS global bundle (every region's roots), cdc_flight/rds_global_ca.py. Its
# text is a separate module, imported only for this name; the pin stays here.
RDS_GLOBAL_SHA256 = "90a6f1a7978a1b1b096dca6c57511ca48d3b8cf052f8e704baf358f34ea2f21b"


def _rds_global_pem():
    from cdc_flight import rds_global_ca

    return rds_global_ca.PEM


# name -> (PEM, or a function returning it, sha256): another region or provider is one
# more entry.
CAS = {
    "rds-global": (_rds_global_pem, RDS_GLOBAL_SHA256),
    "rds-us-east-1": (RDS_US_EAST_1_PEM, RDS_US_EAST_1_SHA256),
}

_baked = None
_ca_path = None
# What this process's PostgreSQL connections asked for and got (reported in cdc_ops'
# run rows; never a path or a DSN fragment).
_requested = None  # (sslmode, ca label)
_in_use = None  # True when every connection negotiated TLS, False if any didn't
_logged = False


class CAError(RuntimeError):
    """A bad CA name or a PEM that doesn't match its pin: raised before connecting."""


def _checked(name):
    if name not in CAS:
        raise CAError(f"unknown PostgreSQL CA {name!r}; known: {sorted(CAS)}")
    pem, pin = CAS[name]
    if callable(pem):
        try:
            pem = pem()
        except ImportError:
            # A broken install without the global CA's module: fail closed, before
            # connecting.
            raise CAError(f"PostgreSQL CA {name!r} is missing from this install") from None
    if hashlib.sha256(pem.encode()).hexdigest() != pin:
        raise CAError(f"PostgreSQL CA {name!r} does not match its pinned sha256")
    return pem


def bake(name):
    """Called once by a stub built with --pg-ca, before anything connects. Repeating
    the same name is a no-op; a different one raises."""
    global _baked
    _checked(name)
    if _baked is not None and _baked != name:
        raise CAError(f"PostgreSQL CA already baked as {_baked!r}")
    _baked = name


def baked():
    return _baked


def ca_file():
    """The baked CA as a file libpq can read (sslrootcert): written once per process,
    mode 0600, removed at exit. libpq reads it on every connect, so it lives as long
    as the process."""
    global _ca_path
    if _baked is None:
        raise CAError("no PostgreSQL CA is baked into this Flight")
    if _ca_path is None:
        pem = _checked(_baked)
        fd, path = tempfile.mkstemp(prefix="cdc_pg_ca_", suffix=".pem")
        try:
            os.fchmod(fd, 0o600)
            os.write(fd, pem.encode())
        finally:
            os.close(fd)
        atexit.register(_remove, path)
        _ca_path = path
    return _ca_path


def _remove(path):
    try:
        os.unlink(path)
    except OSError:
        pass


def label(options):
    """The trust root's label for connection `options` (never a path): the baked name,
    `system` or `dsn` for a DSN's own verify-* setting, `default` for libpq's default
    root file, None unless the mode verifies the server."""
    if not str(options.get("sslmode", "")).startswith("verify-"):
        return None
    if _baked is not None:
        return _baked
    root = options.get("sslrootcert")
    return "system" if root == "system" else "dsn" if root else "default"


def note_options(options):
    """Record what a connection asks for; the first time, log one line (stderr, never
    the DSN): `postgres tls=<sslmode> ca=<label>`."""
    global _requested, _logged
    _requested = (options.get("sslmode"), label(options))
    if not _logged:
        _logged = True
        mode, ca = _requested
        print(f"postgres tls={mode} ca={ca or 'none'}", file=sys.stderr, flush=True)


def note_in_use(in_use):
    """Record whether a connection that opened negotiated TLS."""
    global _in_use
    _in_use = bool(in_use) if _in_use is None else _in_use and bool(in_use)


def fields():
    """This process's TLS state for a cdc_ops run row: tls_mode, tls_ca, tls_in_use
    (NULL when nothing connected)."""
    mode, ca = _requested or (None, None)
    return dict(tls_mode=mode, tls_ca=ca, tls_in_use=_in_use)


def _reset():  # tests only
    global _baked, _ca_path, _requested, _in_use, _logged
    if _ca_path is not None:
        _remove(_ca_path)
    _baked = _ca_path = _requested = _in_use = None
    _logged = False
