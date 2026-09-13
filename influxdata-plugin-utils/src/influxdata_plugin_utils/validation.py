"""Declarative validation of plugin configuration values.

A ``Validator`` describes one config key: the default it falls back to, the
cast that turns it into a usable type, and the checks it must then pass.
``validate`` applies a list of them to a plain dict and returns the result.

Every rejection is a ``ValueError`` naming the key, so a plugin answers a bad
configuration from one ``except`` clause.
"""

import copy
import re

from ._utils import is_blank

__all__ = ["Validator", "validate"]

_MISSING = object()


def _show(value) -> str:
    """A value as it reads in a message: strings quoted, everything else plain."""
    return repr(value) if isinstance(value, (str, bytes)) else str(value)


# check name -> predicate, and how its failure reads
_CHECKS = {
    "eq": (lambda value, other: value == other, "must equal {other}"),
    "ne": (lambda value, other: value != other, "must not equal {other}"),
    "gt": (lambda value, other: value > other, "must be greater than {other}"),
    "gte": (lambda value, other: value >= other, "must be at least {other}"),
    "ge": (lambda value, other: value >= other, "must be at least {other}"),
    "lt": (lambda value, other: value < other, "must be less than {other}"),
    "lte": (lambda value, other: value <= other, "must be at most {other}"),
    "le": (lambda value, other: value <= other, "must be at most {other}"),
    "identity": (lambda value, other: value is other, "must be {other} itself"),
    "is_type_of": (
        lambda value, other: isinstance(value, other),
        "must be of type {other}",
    ),
    "is_in": (lambda value, other: value in other, "must be one of {other}"),
    "is_not_in": (
        lambda value, other: value not in other,
        "must not be one of {other}",
    ),
    "contains": (lambda value, other: other in value, "must contain {other}"),
    "cont": (lambda value, other: other in value, "must contain {other}"),
    "not_contains": (
        lambda value, other: other not in value,
        "must not contain {other}",
    ),
    "len_eq": (lambda value, other: len(value) == other, "must have length {other}"),
    "len_ne": (
        lambda value, other: len(value) != other,
        "must not have length {other}",
    ),
    "len_min": (
        lambda value, other: len(value) >= other,
        "must be at least {other} long",
    ),
    "len_max": (
        lambda value, other: len(value) <= other,
        "must be at most {other} long",
    ),
    "startswith": (
        lambda value, other: value.startswith(other),
        "must start with {other}",
    ),
    "endswith": (
        lambda value, other: value.endswith(other),
        "must end with {other}",
    ),
    "not_startswith": (
        lambda value, other: not value.startswith(other),
        "must not start with {other}",
    ),
    "not_endswith": (
        lambda value, other: not value.endswith(other),
        "must not end with {other}",
    ),
    "regex": (
        lambda value, other: re.search(other, value) is not None,
        "must match {other}",
    ),
    "not_regex": (
        lambda value, other: re.search(other, value) is None,
        "must not match {other}",
    ),
}


class Validator:
    """One rule: which keys it covers, and what they must end up looking like.

    ``required`` asks for a usable value, so a key that arrives blank or
    ``None`` counts as unset. A list, dict or set ``default`` is copied for
    each use, so one rule's default cannot be changed through the values it
    fills in. Checks are named explicitly, so a misspelled one is a
    ``TypeError`` where the rule is written.
    """

    def __init__(
        self,
        *names: str,
        required: bool | None = None,
        must_exist: bool | None = None,
        default=_MISSING,
        apply_default_on_none: bool = False,
        cast=None,
        condition=None,
        when: "Validator | None" = None,
        eq=_MISSING,
        ne=_MISSING,
        gt=_MISSING,
        gte=_MISSING,
        ge=_MISSING,
        lt=_MISSING,
        lte=_MISSING,
        le=_MISSING,
        identity=_MISSING,
        is_type_of=_MISSING,
        is_in=_MISSING,
        is_not_in=_MISSING,
        contains=_MISSING,
        cont=_MISSING,
        not_contains=_MISSING,
        len_eq=_MISSING,
        len_ne=_MISSING,
        len_min=_MISSING,
        len_max=_MISSING,
        startswith=_MISSING,
        endswith=_MISSING,
        not_startswith=_MISSING,
        not_endswith=_MISSING,
        regex=_MISSING,
        not_regex=_MISSING,
    ):
        if not names:
            raise ValueError("a validator needs at least one key name")
        self.names = names
        self.required = bool(required or must_exist)
        self.default = default
        self.apply_default_on_none = apply_default_on_none
        self.cast = cast
        self.condition = condition
        self.when = when
        self.checks = tuple(
            (name, value)
            for name, value in (
                ("eq", eq),
                ("ne", ne),
                ("gt", gt),
                ("gte", gte),
                ("ge", ge),
                ("lt", lt),
                ("lte", lte),
                ("le", le),
                ("identity", identity),
                ("is_type_of", is_type_of),
                ("is_in", is_in),
                ("is_not_in", is_not_in),
                ("contains", contains),
                ("cont", cont),
                ("not_contains", not_contains),
                ("len_eq", len_eq),
                ("len_ne", len_ne),
                ("len_min", len_min),
                ("len_max", len_max),
                ("startswith", startswith),
                ("endswith", endswith),
                ("not_startswith", not_startswith),
                ("not_endswith", not_endswith),
                ("regex", regex),
                ("not_regex", not_regex),
            )
            if value is not _MISSING
        )

    def __repr__(self) -> str:
        return f"Validator({', '.join(repr(name) for name in self.names)})"

    def holds(self, values: dict) -> bool:
        """Would this rule pass against ``values``? Used to answer ``when``.

        A key nobody set does not hold: there is nothing to judge.
        """
        if self.default is _MISSING and any(
            values.get(name, _MISSING) is _MISSING for name in self.names
        ):
            return False
        try:
            self.apply(dict(values))
        except ValueError:
            return False
        return True

    def apply(self, values: dict) -> None:
        """Default, cast and check every key this rule covers, in place."""
        if self.when is not None and not self.when.holds(values):
            return
        for name in self.names:
            self._apply_to(values, name)

    def _apply_to(self, values: dict, name: str) -> None:
        value = values.get(name, _MISSING)
        if value is _MISSING or (value is None and self.apply_default_on_none):
            if self.default is _MISSING:
                if self.required:
                    raise ValueError(f"{name} is required")
                return
            value = self.default
            if isinstance(value, (list, dict, set)):
                value = copy.deepcopy(value)

        if self.required and is_blank(value):
            raise ValueError(f"{name} is required")

        if self.cast is not None:
            try:
                value = self.cast(value)
            except ValueError as exc:
                raise ValueError(f"{name}: {exc}") from exc
            except Exception as exc:
                # a cast is the plugin's own code; its failure rejects the value
                raise ValueError(f"{name}: cannot read {value!r}: {exc}") from exc

        if self.condition is not None:
            try:
                allowed = self.condition(value)
            except Exception as exc:
                raise ValueError(
                    f"{name} cannot be checked with condition: {exc}"
                ) from exc
            if not allowed:
                raise ValueError(f"{name} is not allowed: {value!r}")

        for check, other in self.checks:
            predicate, complaint = _CHECKS[check]
            try:
                passed = predicate(value, other)
            except Exception as exc:
                raise ValueError(
                    f"{name} cannot be checked with {check}: {exc}"
                ) from exc
            if not passed:
                expected = complaint.format(other=_show(other))
                raise ValueError(f"{name} {expected}, got {_show(value)}")

        values[name] = value


def validate(values: dict, validators) -> dict:
    """Return a copy of ``values`` with every validator applied in order."""
    validated = dict(values)
    for validator in validators or ():
        validator.apply(validated)
    return validated
