"""Helpers shared between modules. Not part of the public API."""


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
