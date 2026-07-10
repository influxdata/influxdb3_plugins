"""Package-level smoke tests: importability, public API, version sanity."""

import importlib

import influxdata_plugin_utils as pkg

SUBMODULES = ("cache", "config", "introspection", "parsing", "write")


def test_version_is_valid_pep440():
    from packaging.version import Version

    Version(pkg.__version__)


def test_submodules_import():
    for name in SUBMODULES:
        importlib.import_module(f"influxdata_plugin_utils.{name}")


def test_all_exports_resolve():
    for name in pkg.__all__:
        assert getattr(pkg, name, None) is not None, f"__all__ export {name!r} missing"
