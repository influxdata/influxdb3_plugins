#!/usr/bin/env python3
"""
Validate influxdb3 CLI commands and API usage in plugin README files.

This script extracts CLI commands and API calls from README code blocks and
validates them against current syntax by parsing:
- docs-v2 CLI reference markdown files (for valid/deprecated CLI options)
- OpenAPI specification (for valid API endpoints)

Usage:
    python scripts/validate_influxdb3_syntax.py [--errors-only] [--docs-v2-path PATH]

The script automatically extracts validation rules from source documentation,
eliminating the need for manual configuration updates when CLI or API changes.
"""

import argparse
import os
import re
import sys
from typing import Optional
from pathlib import Path
from typing import NamedTuple

# Optional: yaml for OpenAPI parsing
try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False


class CLIReferenceParser:
    """Parse docs-v2 CLI reference markdown files to extract valid options."""

    def __init__(self, docs_v2_path: Path = None):
        self.docs_v2_path = docs_v2_path
        self.cli_ref_path = None
        if docs_v2_path:
            self.cli_ref_path = docs_v2_path / "content" / "shared" / "influxdb3-cli"
        self._options_cache = {}
        self._deprecated_cache = {}

    def _parse_options_table(self, content: str) -> tuple[list[str], dict[str, str]]:
        """
        Parse markdown options table to extract valid options and deprecated flags.

        Returns (valid_options, deprecated_options) where deprecated_options maps
        option to replacement message.
        """
        valid_options = []
        deprecated_options = {}

        # Match table rows: | short | --long | description |
        # Pattern handles optional short option and various description formats
        row_pattern = r'\|\s*`?(-\w)?`?\s*\|\s*`?(--[\w-]+)`?\s*\|\s*([^|]+)\|'

        for match in re.finditer(row_pattern, content):
            short_opt = match.group(1)
            long_opt = match.group(2)
            description = match.group(3).strip()

            if long_opt:
                valid_options.append(long_opt)

                # Check for deprecated marker
                if 'Deprecated' in description or 'deprecated' in description:
                    # Extract replacement suggestion
                    replacement_match = re.search(r'use\s+`?(--[\w-]+)`?\s+instead', description, re.IGNORECASE)
                    if replacement_match:
                        replacement = replacement_match.group(1)
                        deprecated_options[long_opt] = f"Use {replacement} instead of {long_opt} (deprecated)"
                    else:
                        deprecated_options[long_opt] = f"{long_opt} is deprecated"

            if short_opt:
                valid_options.append(short_opt)

        return valid_options, deprecated_options

    def get_command_options(self, command: str) -> tuple[list[str], dict[str, str]]:
        """
        Get valid options and deprecated options for a CLI command.

        Args:
            command: Command like "create trigger" or "write"

        Returns:
            (valid_options, deprecated_options)
        """
        if command in self._options_cache:
            return self._options_cache[command], self._deprecated_cache.get(command, {})

        if not self.cli_ref_path or not self.cli_ref_path.exists():
            return [], {}

        # Map command to file path
        parts = command.split()
        if len(parts) == 1:
            # Single command like "write" or "query"
            file_path = self.cli_ref_path / f"{parts[0]}.md"
        else:
            # Subcommand like "create trigger"
            file_path = self.cli_ref_path / parts[0] / f"{parts[1]}.md"

        if not file_path.exists():
            return [], {}

        try:
            content = file_path.read_text(encoding='utf-8')
            valid_opts, deprecated_opts = self._parse_options_table(content)
            self._options_cache[command] = valid_opts
            self._deprecated_cache[command] = deprecated_opts
            return valid_opts, deprecated_opts
        except Exception:
            return [], {}

    def get_all_deprecated_options(self) -> dict[str, str]:
        """Scan all CLI reference files and return all deprecated options."""
        all_deprecated = {}

        if not self.cli_ref_path or not self.cli_ref_path.exists():
            return all_deprecated

        for md_file in self.cli_ref_path.rglob("*.md"):
            try:
                content = md_file.read_text(encoding='utf-8')
                _, deprecated = self._parse_options_table(content)
                all_deprecated.update(deprecated)
            except Exception:
                continue

        return all_deprecated


class OpenAPIParser:
    """Parse OpenAPI specification to extract valid API endpoints."""

    def __init__(self, openapi_path: Path = None):
        self.openapi_path = openapi_path
        self._endpoints_cache = None

    def get_valid_endpoints(self) -> list[str]:
        """Extract all valid API endpoints from OpenAPI spec."""
        if self._endpoints_cache is not None:
            return self._endpoints_cache

        if not self.openapi_path or not self.openapi_path.exists():
            return []

        try:
            content = self.openapi_path.read_text(encoding='utf-8')

            if YAML_AVAILABLE:
                # Use proper YAML parsing if available
                spec = yaml.safe_load(content)
                if spec and 'paths' in spec:
                    self._endpoints_cache = list(spec['paths'].keys())
                    return self._endpoints_cache
            else:
                # Fallback: regex parsing for paths section
                endpoints = []
                # Match lines like "  /api/v3/write:" at the paths level
                path_pattern = r'^  (/[a-zA-Z0-9_/{}-]+):\s*$'
                in_paths = False

                for line in content.split('\n'):
                    if line.strip() == 'paths:':
                        in_paths = True
                        continue
                    if in_paths:
                        # Stop at next top-level key
                        if re.match(r'^[a-z]', line):
                            break
                        match = re.match(path_pattern, line)
                        if match:
                            endpoints.append(match.group(1))

                self._endpoints_cache = endpoints
                return endpoints

        except Exception:
            return []

        return []

    def is_valid_endpoint(self, endpoint: str) -> bool:
        """Check if an endpoint path is valid according to OpenAPI spec."""
        valid_endpoints = self.get_valid_endpoints()

        if not valid_endpoints:
            # No spec available, can't validate
            return True

        # Normalize the endpoint
        endpoint = endpoint.rstrip('/')

        for valid in valid_endpoints:
            # Handle path parameters like {request_path}
            pattern = re.sub(r'\{[^}]+\}', r'[^/]+', valid)
            if re.match(f"^{pattern}$", endpoint) or endpoint.startswith(valid.rstrip('/')):
                return True

        return False


# Fallback configuration when docs-v2/OpenAPI not available
DEFAULT_CONFIG = {
    "cli": {
        "deprecated_options": {
            "--plugin-filename": {
                "replacement": "--path",
                "message": "Use --path instead of --plugin-filename (deprecated)"
            }
        },
        "format_rules": {
            "--path": {
                "pattern": r'--path\s+"[^"]+"',
                "message": "--path value should be quoted (for example, --path \"plugin.py\")"
            }
        },
        "recommended_patterns": {
            "gh_prefix": {
                "context": "influxdata/",
                "pattern": r'--path\s+"gh:influxdata/',
                "message": "Consider using gh: prefix for plugins from the library"
            }
        }
    },
    "api": {
        "endpoints": {
            "processing_engine": {
                "base_path": "/api/v3/engine",
                "valid_paths": ["/api/v3/engine/"]
            }
        },
        "deprecated_endpoints": {},
        "format_rules": {}
    },
    "trigger_specs": {
        "valid_formats": [
            {"type": "table", "pattern": r"table:[a-zA-Z_][a-zA-Z0-9_]*"},
            {"type": "all_tables", "pattern": r"all_tables"},
            {"type": "every", "pattern": r"every:\d+[smhdwMy]"},
            {"type": "cron", "pattern": r"cron:.+"},
            {"type": "request", "pattern": r"request:[a-zA-Z_][a-zA-Z0-9_/]*"}
        ]
    }
}


class ValidationIssue(NamedTuple):
    """Represents a validation issue found in a command."""
    level: str  # "error", "warning", "info"
    category: str  # "cli", "api", "trigger_spec"
    message: str
    line_hint: str  # Abbreviated command for context


class InfluxDB3Validator:
    """
    Validates influxdb3 CLI commands and API usage in README files.

    Dynamically extracts validation rules from:
    - docs-v2 CLI reference markdown (deprecated options, valid flags)
    - OpenAPI specification (valid API endpoints)
    """

    def __init__(self, config: dict = None, docs_v2_path: Path = None, openapi_path: Path = None):
        self.config = config or DEFAULT_CONFIG
        self.cli_config = self.config.get("cli", {})
        self.api_config = self.config.get("api", {})
        self.trigger_spec_config = self.config.get("trigger_specs", {})

        # Dynamic parsers for docs-v2 and OpenAPI
        self.cli_parser = CLIReferenceParser(docs_v2_path)
        self.openapi_parser = OpenAPIParser(openapi_path)

        # Merge dynamically-parsed deprecated options with config
        self._dynamic_deprecated = None

    def extract_code_blocks(self, readme_content: str) -> list[tuple[str, str, int]]:
        """
        Extract code blocks from markdown.

        Returns list of (content, language, line_number) tuples.
        """
        blocks = []

        # Match code blocks with optional language specifier
        pattern = r'```(\w*)\n(.*?)```'

        for match in re.finditer(pattern, readme_content, re.DOTALL):
            language = match.group(1) or "unknown"
            content = match.group(2)
            line_num = readme_content[:match.start()].count('\n') + 1
            blocks.append((content, language, line_num))

        return blocks

    def extract_cli_commands(self, block_content: str, block_start: int) -> list[tuple[str, int]]:
        """Extract influxdb3 CLI commands from a code block."""
        commands = []

        if 'influxdb3' not in block_content:
            return commands

        lines = block_content.split('\n')
        current_cmd = []
        cmd_start_line = block_start

        for i, line in enumerate(lines):
            stripped = line.strip()

            # Skip comments and empty lines
            if stripped.startswith('#') or not stripped:
                if not current_cmd:
                    cmd_start_line = block_start + i + 1
                continue

            current_cmd.append(stripped.rstrip('\\').strip())

            # If line doesn't end with backslash, command is complete
            if not line.rstrip().endswith('\\'):
                full_cmd = ' '.join(current_cmd)
                if 'influxdb3' in full_cmd:
                    commands.append((full_cmd, cmd_start_line))
                current_cmd = []
                cmd_start_line = block_start + i + 2

        # Handle any remaining command
        if current_cmd:
            full_cmd = ' '.join(current_cmd)
            if 'influxdb3' in full_cmd:
                commands.append((full_cmd, cmd_start_line))

        return commands

    def extract_api_calls(self, block_content: str, block_start: int) -> list[tuple[str, int]]:
        """Extract curl/API commands from a code block."""
        calls = []

        if 'curl' not in block_content and '/api/v3/' not in block_content:
            return calls

        lines = block_content.split('\n')
        current_cmd = []
        cmd_start_line = block_start

        for i, line in enumerate(lines):
            stripped = line.strip()

            if stripped.startswith('#') or not stripped:
                if not current_cmd:
                    cmd_start_line = block_start + i + 1
                continue

            current_cmd.append(stripped.rstrip('\\').strip())

            if not line.rstrip().endswith('\\'):
                full_cmd = ' '.join(current_cmd)
                if 'curl' in full_cmd or '/api/v3/' in full_cmd:
                    calls.append((full_cmd, cmd_start_line))
                current_cmd = []
                cmd_start_line = block_start + i + 2

        if current_cmd:
            full_cmd = ' '.join(current_cmd)
            if 'curl' in full_cmd or '/api/v3/' in full_cmd:
                calls.append((full_cmd, cmd_start_line))

        return calls

    def _get_deprecated_options(self) -> dict[str, str]:
        """Get deprecated options from both config and dynamic parsing."""
        if self._dynamic_deprecated is None:
            # Start with config-based deprecated options
            self._dynamic_deprecated = {}
            for opt, info in self.cli_config.get("deprecated_options", {}).items():
                self._dynamic_deprecated[opt] = info.get("message", f"{opt} is deprecated")

            # Add dynamically parsed deprecated options from docs-v2
            dynamic = self.cli_parser.get_all_deprecated_options()
            self._dynamic_deprecated.update(dynamic)

        return self._dynamic_deprecated

    def _extract_cli_subcommand(self, command: str) -> str:
        """Extract the subcommand from an influxdb3 command (e.g., 'create trigger')."""
        match = re.search(r'influxdb3\s+(\w+)(?:\s+(\w+))?', command)
        if match:
            cmd = match.group(1)
            subcmd = match.group(2)
            if subcmd and cmd in ['create', 'delete', 'update', 'show', 'enable', 'disable', 'install', 'test']:
                return f"{cmd} {subcmd}"
            return cmd
        return ""

    def validate_cli_command(self, command: str, readme_path: str = "") -> list[ValidationIssue]:
        """Validate a single influxdb3 CLI command."""
        issues = []
        line_hint = command[:80] + "..." if len(command) > 80 else command

        # Get deprecated options (merged from config and docs-v2 parsing)
        deprecated_options = self._get_deprecated_options()
        format_rules = self.cli_config.get("format_rules", {})
        recommended_patterns = self.cli_config.get("recommended_patterns", {})

        # Check for deprecated options
        for deprecated_opt, message in deprecated_options.items():
            if deprecated_opt in command:
                issues.append(ValidationIssue(
                    level="error",
                    category="cli",
                    message=message,
                    line_hint=line_hint
                ))

        # Check format rules
        for option, rule in format_rules.items():
            if option in command:
                if not re.search(rule["pattern"], command):
                    issues.append(ValidationIssue(
                        level="error",
                        category="cli",
                        message=rule["message"],
                        line_hint=line_hint
                    ))

        # Check recommended patterns (warnings only)
        for pattern_name, rule in recommended_patterns.items():
            context = rule.get("context", "")
            if context and context not in readme_path:
                continue

            if "--path" in command and "gh:" not in command:
                if "influxdata/" in readme_path:
                    issues.append(ValidationIssue(
                        level="warning",
                        category="cli",
                        message=rule["message"],
                        line_hint=line_hint
                    ))

        # Validate trigger specs
        if "--trigger-spec" in command:
            issues.extend(self.validate_trigger_spec(command, line_hint))

        return issues

    def validate_trigger_spec(self, command: str, line_hint: str) -> list[ValidationIssue]:
        """Validate trigger specification format."""
        issues = []

        # Extract trigger spec value
        match = re.search(r'--trigger-spec\s+"([^"]+)"', command)
        if not match:
            match = re.search(r'--trigger-spec\s+(\S+)', command)

        if not match:
            return issues

        spec_value = match.group(1)
        valid_formats = self.trigger_spec_config.get("valid_formats", [])

        # Check if spec matches any valid format
        is_valid = False
        for format_def in valid_formats:
            if re.match(format_def["pattern"], spec_value):
                is_valid = True
                break

        if not is_valid and valid_formats:
            valid_types = [f["type"] for f in valid_formats]
            issues.append(ValidationIssue(
                level="warning",
                category="trigger_spec",
                message=f"Trigger spec '{spec_value}' may not match expected formats: {', '.join(valid_types)}",
                line_hint=line_hint
            ))

        return issues

    def _extract_api_endpoint(self, call: str) -> str:
        """Extract API endpoint path from a curl command or URL."""
        # Match patterns like /api/v3/write or http://localhost:8181/api/v3/query
        patterns = [
            r'https?://[^/\s]+(/api/v[0-9]/[^\s"\']+)',  # Full URL
            r'https?://[^/\s]+(/[^\s"\']+)',  # Any path after host
            r'"(/api/v[0-9]/[^\s"\']+)"',  # Quoted path
            r'\s(/api/v[0-9]/\S+)',  # Unquoted path
        ]

        for pattern in patterns:
            match = re.search(pattern, call)
            if match:
                endpoint = match.group(1)
                # Clean up query params
                endpoint = endpoint.split('?')[0]
                return endpoint

        return ""

    def validate_api_call(self, call: str, readme_path: str = "") -> list[ValidationIssue]:
        """Validate an API call (curl command or endpoint reference)."""
        issues = []
        line_hint = call[:80] + "..." if len(call) > 80 else call

        deprecated_endpoints = self.api_config.get("deprecated_endpoints", {})
        format_rules = self.api_config.get("format_rules", {})

        # Check for deprecated endpoints (from config)
        for endpoint, info in deprecated_endpoints.items():
            if endpoint in call:
                issues.append(ValidationIssue(
                    level="error",
                    category="api",
                    message=info.get("message", f"Deprecated endpoint: {endpoint}"),
                    line_hint=line_hint
                ))

        # Validate endpoint against OpenAPI spec (only for InfluxDB endpoints)
        endpoint = self._extract_api_endpoint(call)
        if endpoint:
            # Only validate InfluxDB API endpoints, skip external APIs
            is_influxdb_endpoint = (
                endpoint.startswith('/api/v') or
                endpoint.startswith('/write') or
                endpoint.startswith('/query') or
                endpoint.startswith('/ping') or
                endpoint.startswith('/health') or
                endpoint.startswith('/metrics') or
                'localhost' in call or
                '127.0.0.1' in call or
                '8181' in call  # Default InfluxDB port
            )

            if is_influxdb_endpoint:
                valid_endpoints = self.openapi_parser.get_valid_endpoints()
                if valid_endpoints and not self.openapi_parser.is_valid_endpoint(endpoint):
                    issues.append(ValidationIssue(
                        level="warning",
                        category="api",
                        message=f"Endpoint '{endpoint}' not found in OpenAPI spec",
                        line_hint=line_hint
                    ))

        # Check API format rules
        for rule_name, rule in format_rules.items():
            if rule.get("applies_to", "") in call or not rule.get("applies_to"):
                pattern = rule.get("pattern", "")
                if pattern and not re.search(pattern, call):
                    # Only warn if the rule context applies
                    if "Authorization" in rule_name and "curl" in call:
                        issues.append(ValidationIssue(
                            level="warning",
                            category="api",
                            message=rule["message"],
                            line_hint=line_hint
                        ))

        return issues

    def validate_readme(self, readme_path: Path) -> dict:
        """
        Validate all CLI commands and API calls in a README file.

        Returns dict with issues and counts.
        """
        try:
            content = readme_path.read_text(encoding='utf-8')
        except Exception as e:
            return {
                "issues": [ValidationIssue(
                    level="error",
                    category="file",
                    message=f"Failed to read file: {e}",
                    line_hint=str(readme_path)
                )],
                "cli_count": 0,
                "api_count": 0
            }

        all_issues = []
        cli_count = 0
        api_count = 0

        code_blocks = self.extract_code_blocks(content)

        for block_content, language, line_num in code_blocks:
            # Extract and validate CLI commands
            cli_commands = self.extract_cli_commands(block_content, line_num)
            cli_count += len(cli_commands)
            for cmd, cmd_line in cli_commands:
                all_issues.extend(self.validate_cli_command(cmd, str(readme_path)))

            # Extract and validate API calls
            api_calls = self.extract_api_calls(block_content, line_num)
            api_count += len(api_calls)
            for call, call_line in api_calls:
                all_issues.extend(self.validate_api_call(call, str(readme_path)))

        return {
            "issues": all_issues,
            "cli_count": cli_count,
            "api_count": api_count
        }


def find_readme_files(base_dir: Path) -> list[Path]:
    """Find all plugin README files in the influxdata directory."""
    readme_files = []
    influxdata_dir = base_dir / "influxdata"

    if not influxdata_dir.exists():
        return readme_files

    for plugin_dir in influxdata_dir.iterdir():
        if plugin_dir.is_dir() and plugin_dir.name != "library":
            readme_path = plugin_dir / "README.md"
            if readme_path.exists():
                readme_files.append(readme_path)

    return sorted(readme_files)


def find_docs_v2_path(script_dir: Path) -> Optional[Path]:
    """Try to find the docs-v2 repository path."""
    # Check common relative paths from the plugins repo
    candidates = [
        script_dir.parent.parent / "docs-v2",
        script_dir.parent.parent.parent / "docs-v2",
    ]

    # Also check for worktree structure (project-name/docs-v2-branch)
    parent = script_dir.parent.parent
    if parent.exists():
        for item in parent.iterdir():
            if item.is_dir() and 'docs-v2' in item.name:
                candidates.append(item)

    for path in candidates:
        if path.exists() and (path / "content" / "shared" / "influxdb3-cli").exists():
            return path

    return None


def find_openapi_path(docs_v2_path: Path = None) -> Optional[Path]:
    """Try to find the OpenAPI specification file."""
    if docs_v2_path:
        # Check standard location in docs-v2
        openapi_path = docs_v2_path / "api-docs" / "influxdb3" / "core" / "v3" / "ref.yml"
        if openapi_path.exists():
            return openapi_path

    return None


def main():
    parser = argparse.ArgumentParser(
        description="Validate influxdb3 CLI commands and API usage in plugin README files"
    )
    parser.add_argument(
        "--errors-only",
        action="store_true",
        help="Only report errors, suppress warnings and info messages"
    )
    parser.add_argument(
        "--docs-v2-path",
        type=str,
        help="Path to docs-v2 repository for CLI reference parsing"
    )
    parser.add_argument(
        "--openapi-path",
        type=str,
        help="Path to OpenAPI spec file for API validation"
    )
    parser.add_argument(
        "--readme",
        type=str,
        help="Validate a specific README file instead of all plugins"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show detailed output including command counts"
    )
    parser.add_argument(
        "--cli-only",
        action="store_true",
        help="Only validate CLI commands, skip API validation"
    )
    parser.add_argument(
        "--api-only",
        action="store_true",
        help="Only validate API calls, skip CLI validation"
    )

    args = parser.parse_args()

    # Determine base directory
    script_dir = Path(__file__).parent
    base_dir = script_dir.parent

    # Find docs-v2 path for CLI reference parsing
    docs_v2_path = None
    if args.docs_v2_path:
        docs_v2_path = Path(args.docs_v2_path)
    else:
        docs_v2_path = find_docs_v2_path(script_dir)

    # Find OpenAPI spec path
    openapi_path = None
    if args.openapi_path:
        openapi_path = Path(args.openapi_path)
    elif docs_v2_path:
        openapi_path = find_openapi_path(docs_v2_path)

    # Report what sources are available
    if args.verbose:
        if docs_v2_path:
            print(f"Using docs-v2 CLI reference: {docs_v2_path}")
        else:
            print("Note: docs-v2 path not found, using fallback config for CLI validation")

        if openapi_path:
            print(f"Using OpenAPI spec: {openapi_path}")
        else:
            print("Note: OpenAPI spec not found, API endpoint validation will be limited")
        print()

    # Create validator with dynamic parsers
    validator = InfluxDB3Validator(
        config=DEFAULT_CONFIG,
        docs_v2_path=docs_v2_path,
        openapi_path=openapi_path
    )

    # Find README files to validate
    if args.readme:
        readme_files = [Path(args.readme)]
    else:
        readme_files = find_readme_files(base_dir)

    if not readme_files:
        print("No README files found to validate")
        return 0

    print(f"Validating CLI commands and API usage in {len(readme_files)} README file(s)...\n")

    total_errors = 0
    total_warnings = 0
    total_cli_commands = 0
    total_api_calls = 0
    files_with_issues = []

    for readme_path in readme_files:
        result = validator.validate_readme(readme_path)
        issues = result["issues"]

        total_cli_commands += result["cli_count"]
        total_api_calls += result["api_count"]

        # Filter by category if requested
        if args.cli_only:
            issues = [i for i in issues if i.category == "cli" or i.category == "trigger_spec"]
        elif args.api_only:
            issues = [i for i in issues if i.category == "api"]

        # Filter issues based on --errors-only flag
        if args.errors_only:
            issues = [i for i in issues if i.level == "error"]

        errors = [i for i in issues if i.level == "error"]
        warnings = [i for i in issues if i.level == "warning"]

        total_errors += len(errors)
        total_warnings += len(warnings)

        if issues:
            files_with_issues.append((readme_path, issues, result))

    # Print results
    for readme_path, issues, result in files_with_issues:
        rel_path = readme_path.relative_to(base_dir)

        errors = [i for i in issues if i.level == "error"]
        warnings = [i for i in issues if i.level == "warning"]

        if errors:
            print(f"❌ {rel_path}:")
        elif warnings:
            print(f"⚠️  {rel_path}:")

        for issue in issues:
            prefix = "  ERROR:" if issue.level == "error" else "  WARNING:"
            category_tag = f"[{issue.category}]"
            print(f"{prefix} {category_tag} {issue.message}")
            if args.verbose:
                print(f"    Context: {issue.line_hint}")
        print()

    # Print summary
    print("=" * 60)
    print("CLI AND API VALIDATION SUMMARY")
    print("=" * 60)
    print(f"Files validated: {len(readme_files)}")
    print(f"CLI commands found: {total_cli_commands}")
    print(f"API calls found: {total_api_calls}")
    print(f"Errors found: {total_errors}")
    if not args.errors_only:
        print(f"Warnings found: {total_warnings}")

    # Show validation sources
    sources = []
    if docs_v2_path:
        sources.append("docs-v2 CLI reference")
    if openapi_path:
        sources.append("OpenAPI spec")
    if sources:
        print(f"Validation sources: {', '.join(sources)}")
    else:
        print("Validation sources: fallback config only")
    print()

    if total_errors > 0:
        print(f"❌ Validation FAILED with {total_errors} error(s)")
        return 1
    elif total_warnings > 0 and not args.errors_only:
        print(f"⚠️  Validation passed with {total_warnings} warning(s)")
        return 0
    else:
        print("✅ All CLI commands and API usage validated successfully")
        return 0


if __name__ == "__main__":
    sys.exit(main())
