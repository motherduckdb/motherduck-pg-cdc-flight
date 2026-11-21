"""Closed PostgreSQL text-output contract shared by bootstrap and streaming."""

from cdc_flight.format2 import OUTPUT_SETTINGS

# server_encoding is read-only: bootstrap verifies UTF8 rather than setting it.
DEFAULT_SETTINGS = tuple(
    zip(
        OUTPUT_SETTINGS,
        ("UTC", "ISO, YMD", "postgres", "3", "hex", "C", "base64", "content", "UTF8", "UTF8"),
        strict=True,
    )
)


def connection_settings(config, options):
    """Append authoritative GUCs after DSN options, without exposing the DSN.

    libpq splits the options string on unescaped whitespace. Escape each value
    so even a recorded value containing spaces remains one -c argument.
    """
    binding = getattr(config, "text_binding", None)
    settings = dict(binding.settings if binding is not None else DEFAULT_SETTINGS)
    if set(settings) != set(OUTPUT_SETTINGS):
        raise ValueError("the complete text-output settings binding is required")
    additions = []
    for name in OUTPUT_SETTINGS:
        if name == "server_encoding":
            continue
        value = settings[name]
        escaped = "".join("\\" + c if c.isspace() or c == "\\" else c for c in value)
        additions.append(f"-c {name}={escaped}")
    return " ".join(filter(None, (options, *additions)))
