"""Helpers shared between modules. Not part of the public API."""

MAX_SHOWN_CHARS = 200


def shown(value) -> str:
    """A value as it reads in a message: text quoted, and never unbounded.

    A message travels into an HTTP response and a log line, so a value the
    caller chose the size of is cut rather than copied whole.
    """
    if isinstance(value, (str, bytes, bytearray)):
        if len(value) > MAX_SHOWN_CHARS:
            # sliced before repr, so a huge value is never rendered in full
            return f"{value[:MAX_SHOWN_CHARS]!r}... ({len(value)} in all)"
        return repr(value)
    text = str(value)
    if len(text) > MAX_SHOWN_CHARS:
        return f"{text[:MAX_SHOWN_CHARS]}... ({len(text)} in all)"
    return text


def as_text(value) -> str:
    """Render a raw name or value as text.

    ``bytes`` are decoded: ``str(b"secret")`` would keep the ``b'...'`` repr.
    """
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", "replace")
    return str(value)


def is_blank(value) -> bool:
    """A value that arrived empty and so counts as not provided."""
    return value is None or (isinstance(value, str) and not value.strip())


def require_choice(value: str, choices: tuple[str, ...], label: str) -> None:
    """Reject a string argument that is not one of ``choices``."""
    if value not in choices:
        raise ValueError(f"Invalid {label} {value!r}. Supported: {', '.join(choices)}")
