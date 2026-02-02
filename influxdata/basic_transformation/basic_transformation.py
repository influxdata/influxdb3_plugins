"""
{
    "plugin_type": ["scheduled", "onwrite"],
    "scheduled_args_config": [
        {
            "name": "measurement",
            "example": "temperature",
            "description": "The InfluxDB measurement to query for historical data.",
            "required": true
        },
        {
            "name": "window",
            "example": "30d",
            "description": "Historical window duration for data retrieval. Format: <number><unit> where unit is s, min, h, d, w, m, q, y.",
            "required": true
        },
        {
            "name": "target_measurement",
            "example": "transformed_temperature",
            "description": "Destination measurement for storing transformed data.",
            "required": true
        },
        {
            "name": "names_transformations",
            "example": "room:'lower snake'.temp:'upper camel'.name:'custom_replacement kebab'",
            "description": "Rules for transforming field and tag names. Format: 'field1:'transform1 transform2'.pattern_name:'transform3 transform4'.",
            "required": false
        },
        {
            "name": "values_transformations",
            "example": "temp:'convert_degC_to_degF'.hum:'upper normalize_whitespace'.something:'lower remove_digits'",
            "description": "Rules for transforming field values. Format: 'field1:'transform1 transform2'.pattern:'transform3'.",
            "required": false
        },
        {
            "name": "target_database",
            "example": "transformed_db",
            "description": "Optional InfluxDB database name for writing transformed data.",
            "required": false
        },
        {
            "name": "included_fields",
            "example": "temp.hum.location",
            "description": "Dot-separated list of field and tag names to include in the query.",
            "required": false
        },
        {
            "name": "excluded_fields",
            "example": "co.h-u_m2.device_id",
            "description": "Dot-separated list of field and tag names to exclude from the query.",
            "required": false
        },
        {
            "name": "dry_run",
            "example": "true",
            "description": "If 'true', simulates the transformation without writing to the database. Defaults to 'false'.",
            "required": false
        },
        {
            "name": "custom_replacements",
            "example": "replace_space_underscore:' =_'.cust_replace:'Some text=Another text'",
            "description": "Custom replacement rules for transformations. Format: 'replace_space_underscore:' =_'.cust_replace:'Some text=Another text'.",
            "required": false
        },
        {
            "name": "custom_regex",
            "example": "regex_temp:'temp%'",
            "description": "Custom regex patterns for applying transformations. Format: 'regex_temp:'temp%'. Only '%' (zero, one or more) and '_' (exactly one) are allowed in regex patterns.",
            "required": false
        },
        {
            "name": "filters",
            "example": "temp:'>=101'.hum:'<=182'",
            "description": "Filters for querying specific data. Format: 'field:'=value'.field2:'>value2'. Supported operators: '=', '!=', '>', '<', '>=', '<='.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "config.toml",
            "description": "Path to config file to override args. Format: 'config.toml'.",
            "required": false
        }
    ],
    "onwrite_args_config": [
                {
            "name": "measurement",
            "example": "temperature",
            "description": "The InfluxDB measurement to query for historical data.",
            "required": true
        },
        {
            "name": "target_measurement",
            "example": "transformed_temperature",
            "description": "Destination measurement for storing transformed data.",
            "required": true
        },
        {
            "name": "names_transformations",
            "example": "room:'lower snake'.temp:'upper camel'.name:'custom_replacement kebab'",
            "description": "Rules for transforming field and tag names. Format: 'field1:'transform1 transform2'.pattern_name:'transform3 transform4'.",
            "required": false
        },
        {
            "name": "values_transformations",
            "example": "temp:'convert_degC_to_degF'.hum:'upper normalize_whitespace'.something:'lower remove_digits'",
            "description": "Rules for transforming field values. Format: 'field1:'transform1 transform2'.pattern:'transform3'.",
            "required": false
        },
        {
            "name": "target_database",
            "example": "transformed_db",
            "description": "Optional InfluxDB database name for writing transformed data.",
            "required": false
        },
        {
            "name": "included_fields",
            "example": "temp.hum.location",
            "description": "Dot-separated list of field and tag names to include in the query.",
            "required": false
        },
        {
            "name": "excluded_fields",
            "example": "co.h-u_m2.device_id",
            "description": "Dot-separated list of field and tag names to exclude from the query.",
            "required": false
        },
        {
            "name": "dry_run",
            "example": "true",
            "description": "If 'true', simulates the transformation without writing to the database. Defaults to 'false'.",
            "required": false
        },
        {
            "name": "custom_replacements",
            "example": "replace_space_underscore:' =_'.cust_replace:'Some text=Another text'",
            "description": "Custom replacement rules for transformations. Format: 'replace_space_underscore:' =_'.cust_replace:'Some text=Another text'.",
            "required": false
        },
        {
            "name": "custom_regex",
            "example": "regex_temp:'temp%'",
            "description": "Custom regex patterns for applying transformations. Format: 'regex_temp:'temp%'. Only '%' (zero, one or more) and '_' (exactly one) are allowed in regex patterns.",
            "required": false
        },
        {
            "name": "filters",
            "example": "temp:'>=101'.hum:'<=182'",
            "description": "Filters for querying specific data. Format: 'field:'=value'.field2:'>value2'. Supported operators: '=', '!=', '>', '<', '>=', '<='.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "config.toml",
            "description": "Path to config file to override args. Format: 'config.toml'.",
            "required": false
        }
    ]
}
"""

import operator
import os
import random
import re
import time
import tomllib
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
import math

from pint import UnitRegistry

_OP_FUNCS = {
    ">": operator.gt,
    "<": operator.lt,
    ">=": operator.ge,
    "<=": operator.le,
    "=": operator.eq,
    "!=": operator.ne,
}


def to_lowercase(s: str) -> str:
    return s.lower()


def to_uppercase(s: str) -> str:
    return s.upper()


def spaces_to_underscores(s: str) -> str:
    return s.replace(" ", "_")


def remove_spaces(s: str) -> str:
    return s.replace(" ", "")


def keep_alnum_underscore(s: str) -> str:
    return re.sub(r"[^\w]", "", s)


def collapse_underscores(s: str) -> str:
    return re.sub(r"_+", "_", s)


def trim_underscores(s: str) -> str:
    return s.strip("_")


def to_snake_case(s: str) -> str:
    s = re.sub(r"[\s\-]+", "_", s)
    s = re.sub(r"(?<!^)(?=[A-Z])", "_", s)
    s = re.sub(r"[^\w]", "", s)
    return s.lower()


def to_camel_case(s: str) -> str:
    s = re.sub(r"[^\w\s]", " ", s)
    words = s.split()
    if not words:
        return s
    return words[0].lower() + "".join(word.capitalize() for word in words[1:])


def to_pascal_case(s: str) -> str:
    s = re.sub(r"[^\w\s]", " ", s)
    words = s.split()
    return "".join(word.capitalize() for word in words)


def to_kebab_case(s: str) -> str:
    s = re.sub(r"(?<!^)(?=[A-Z])", "-", s)
    s = re.sub(r"[\s_-]+", "-", s)
    s = re.sub(r"[^\w-]", "", s)
    return s.lower()


def to_title_case(s: str) -> str:
    return " ".join(word.capitalize() for word in re.split(r"[\s_-]+", s))


def reverse_string(s: str) -> str:
    return s[::-1]


def capitalize_first(s: str) -> str:
    return s[0].upper() + s[1:] if s else s


def capitalize_words(s: str) -> str:
    return " ".join(word.capitalize() for word in re.split(r"[\s_-]+", s))


def normalize_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip())


def remove_digits(s: str) -> str:
    return re.sub(r"\d", "", s)


def remove_punctuation(s: str) -> str:
    return re.sub(r"[^\w\s]", "", s)


def keep_alphanumeric(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]", "", s)


def remove_special_chars(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9\s_-]", "", s)


def normalize_dashes(s: str) -> str:
    return re.sub(r"[\-_]+", "-", s)


def normalize_underscores(s: str) -> str:
    return re.sub(r"[\-_\s]+", "_", s)


def absolute_value(value: int | float) -> float:
    return abs(value)


def round_to_decimal(value: int | float) -> float:
    return round(value, 2)


def square_root(value: int | float) -> float:
    return math.sqrt(value)


def logarithm(value: int | float) -> float:
    return math.log(value)


def boolean_to_int(s) -> int:
    if s in ('true', 'yes', '1', 'on', True):
        return 1
    elif s in ('false', 'no', '0', 'off', False):
        return 0
    return s


def clamp_min_zero(value: int | float) -> float | int:
    return max(0, value)


def clamp_max_hundred(value: int | float) -> float | int:
    return min(100, value)


def extract_numbers_only(s: str) -> str:
    return re.sub(r"[^\d]", "", s)


def extract_letters_only(s: str) -> str:
    return re.sub(r"[^a-zA-Z]", "", s)


def to_percentage(value: int | float) -> float | int:
    """Multiplies by 100 (to convert to percentage)"""
    return value * 100


def from_percentage(value: int | float) -> float | int:
    """Divides by 100 (converts from percentage to decimal)"""
    return value / 100


def floor_value(value: int | float) -> int:
    """Rounds down to the nearest integer"""
    return math.floor(value)


def ceil_value(value: int | float) -> int:
    """Rounds up to the nearest integer"""
    return math.ceil(value)


# Transformation functions
TRANSFORMATIONS = {
    "lower": to_lowercase,
    "upper": to_uppercase,
    "space_to_underscore": spaces_to_underscores,
    "remove_space": remove_spaces,
    "alnum_underscore_only": keep_alnum_underscore,
    "collapse_underscore": collapse_underscores,
    "trim_underscore": trim_underscores,
    "snake": to_snake_case,
    "camel": to_camel_case,
    "pascal": to_pascal_case,
    "kebab": to_kebab_case,
    "title": to_title_case,
    "reverse": reverse_string,
    "capitalize_first": capitalize_first,
    "capitalize_words": capitalize_words,
    "normalize_whitespace": normalize_whitespace,
    "remove_digits": remove_digits,
    "remove_punctuation": remove_punctuation,
    "keep_alphanumeric": keep_alphanumeric,
    "remove_special_chars": remove_special_chars,
    "normalize_dashes": normalize_dashes,
    "normalize_underscores": normalize_underscores,
    "abs": absolute_value,
    "round2": round_to_decimal,
    "sqrt": square_root,
    "ln": logarithm,
    "boolean_to_int": boolean_to_int,
    "clamp_min_zero": clamp_min_zero,
    "clamp_max_hundred": clamp_max_hundred,
    "extract_numbers_only": extract_numbers_only,
    "extract_letters_only": extract_letters_only,
    "to_percentage": to_percentage,
    "from_percentage": from_percentage,
    "floor": floor_value,
    "ceil": ceil_value,
}


def parse_time_interval(raw: str, task_id: str) -> timedelta:
    """
    Parses the interval string from raw into a datetime.timedelta.

    Supports time units:
      - seconds: "s"
      - minutes: "min"
      - hours: "h"
      - days: "d"
      - weeks: "w"
      - months: "m"      (approximate: 1 month ≈ 30.42 days)
      - quarters: "q"    (approximate: 1 quarter ≈ 91.25 days)
      - years: "y"       (approximate: 1 year = 365 days)

    Args:
        raw (str): The interval string to be parsed.
        task_id (str): Task identifier for logging context.

    Returns:
        timedelta: The parsed duration as a datetime.timedelta.

    Raises:
        Exception: If the format is invalid, unit unsupported.
    """
    unit_mapping: dict = {
        "s": "seconds",
        "min": "minutes",
        "h": "hours",
        "d": "days",
        "w": "weeks",
        "m": "days",  # months -> days
        "q": "days",  # quarters -> days
        "y": "days",  # years -> days
    }
    # Approximate conversions to days for month, quarter, year
    day_conversions: dict = {
        "m": 30.42,  # average days in a month
        "q": 91.25,  # average days in a quarter
        "y": 365.0,  # days in a year
    }
    valid_units = unit_mapping.keys()

    if not isinstance(raw, str):
        raise Exception(
            f"[{task_id}] Invalid {raw} type: expected string like '10min', got {type(raw)}"
        )

    match = re.fullmatch(r"(\d+)([a-zA-Z]+)", raw.strip())
    if not match:
        raise Exception(
            f"[{task_id}] Invalid raw format: '{raw}'. Expected format '<number><unit>', e.g. '10min', '2d'."
        )

    number_part, unit = match.groups()
    try:
        magnitude = int(number_part)
    except ValueError:
        raise Exception(f"[{task_id}] Invalid number in {raw}: '{number_part}'")

    unit = unit.lower()
    if unit not in valid_units:
        raise Exception(f"[{task_id}] Unsupported unit '{unit}' in raw: '{raw}'")

    # Build timedelta
    if unit in day_conversions:
        # months, quarters, years -> approximate days
        days_approx = int(magnitude * day_conversions[unit])
        if days_approx < 1:
            raise Exception(
                f"[{task_id}] Computed days < 1 for {magnitude}{unit} in raw"
            )
        return timedelta(days=days_approx)

    # For other units, use direct timedelta arguments
    if unit == "s":
        return timedelta(seconds=magnitude)
    elif unit == "min":
        return timedelta(minutes=magnitude)
    elif unit == "h":
        return timedelta(hours=magnitude)
    elif unit == "d":
        return timedelta(days=magnitude)
    elif unit == "w":
        return timedelta(weeks=magnitude)

    raise Exception(f"[{task_id}] Error while converting time interval '{raw}'")


def parse_fields(args: dict, key: str) -> list[str]:
    """Splits a dot-separated string into a list of strings or reads from a config file."""
    val_input: str | None | list = args.get(key, None)
    if not val_input:
        return []

    # Use parameters from config file
    if args["use_config_file"]:
        if isinstance(val_input, list):
            return val_input
        else:
            raise Exception(f"Invalid value for {key}: expected list, got {type(val_input)}")

    return val_input.split(".")


def parse_transformation_rules(
    influxdb3_local, args: dict, key: str, task_id: str
) -> dict[str, list[str]]:
    """
    Parses a string of the form 'field:"val1 val2 val3".field2:"val1 val2 val3"' into a dictionary or uses value from config file.

    Args:
        influxdb3_local: InfluxDB client instance.
        args (dict): Input arguments containing the string to be parsed.
        key (str): Key in args containing the string to be parsed.
        task_id (str): Task identifier for logging context.

    Returns:
        dict: A dictionary where keys are field names and values are lists of values.

    Raises:
        ValueError: If the dictionary remains empty after parsing.
    """
    result: dict = {}
    input_val: str | None | dict = args.get(key, None)
    if not input_val:
        return result

    # Use parameters from config file
    if args["use_config_file"]:
        if isinstance(input_val, dict):
            return input_val
        else:
            raise Exception(
                f"[{task_id}] Invalid {key} type: expected dict, got {type(result)}"
            )

    parts: list = input_val.split(".")

    for part in parts:
        if not part:
            continue
        if ":" not in part:
            influxdb3_local.warn(
                f"[{task_id}] Part {part} should contain ':', skipping"
            )
            continue
        name, values_str = part.split(":")
        if values_str[0] == values_str[-1] and values_str[0] in ("'", '"'):
            values_str = values_str[1:-1]

        values_list: list = values_str.split(" ")
        result[name] = values_list

    if not result:
        raise Exception(f"[{task_id}] No valid transformation pairs found for {key}.")

    return result


def parse_field_filters(
    influxdb3_local, args: dict, key: str, task_id: str
) -> list[list]:
    """
    Parse a filter string into a list of (field, operator, value) lists or uses value from config file.

    The input string should be in the form:
        'field:>=10.field2:="abc".field3:!=5.5'
    where:
        - field     — the measurement or tag key
        - operator  — one of >=, <=, !=, >, <, =
        - value     — a literal int, float, or quoted string

    Args:
        influxdb3_local: InfluxDB client instance for logging warnings.
        args (dict): Dictionary of input arguments.
        key (str): Key in `args` whose value is the filter string.
        task_id (str): Identifier for logging context.

    Returns:
        list[list[str, str, str | int | float]]:
            A list of lists: [field_name, operator, value].

    Raises:
        ValueError: If no valid filters are found in the input string.
    """
    operators: list = [">=", "<=", "!=", ">", "<", "="]
    result: list[list] = []

    raw: str | None | list = args.get(key)
    if not raw:
        return result

    # Use parameters from config file
    if args["use_config_file"]:
        if isinstance(raw, list):
            return raw
        else:
            raise Exception(
                f"[{task_id}] Invalid {key} type: expected list, got {type(result)}"
            )

    parts: list = raw.split(".")

    for part in parts:
        if ":" not in part:
            influxdb3_local.warn(
                f"[{task_id}] Skipping invalid part without ':': '{part}'"
            )
            continue

        field, expr = part.split(":", 1)
        field = field.strip()
        expr = expr.strip()
        if expr[0] == expr[-1] and expr[0] in ("'", '"'):
            expr = expr[1:-1]

        # Find matching operator
        op = next((o for o in operators if expr.startswith(o)), None)
        if not op:
            influxdb3_local.warn(f"[{task_id}] No valid operator in part: '{part}'")
            continue

        value_str: str = expr[len(op) :].strip()

        # Strip quotes from string literals
        if (
            len(value_str) >= 2
            and value_str[0] == value_str[-1]
            and value_str[0] in ("'", '"')
        ):
            value_str = value_str[1:-1]

        # Convert to int or float if possible
        if re.fullmatch(r"-?\d+", value_str):
            value: str | int | float = int(value_str)
        elif re.fullmatch(r"-?\d+\.\d+", value_str):
            value = float(value_str)
        else:
            value = value_str

        result.append([field, op, value])

    if not result:
        raise Exception(f"[{task_id}] No valid filters parsed for key '{key}'")

    return result


def parse_custom_replacements(
    influxdb3_local, args: dict, task_id: str
) -> dict[str, list]:
    """
    Parses a custom transformation string into a dictionary of (left, right) lists or uses value from config file.

    The expected string input format is:
        'field:"left_part=right_part".field2:"left2=right2"'

    Each part:
        - Uses ':' to separate field name and value
        - Wraps value in single or double quotes
        - Contains one '=' character to separate left and right parts

    Args:
        influxdb3_local: InfluxDB client instance for logging.
        args (dict): dictionary of input arguments.
        task_id (str): Identifier used for logging context.

    Returns:
        dict[str, list[str, str]]: A dictionary where:
            - keys are field names
            - values are lists (left, right), parsed from the quoted value.

    Raises:
        ValueError: If no valid transformations are found in the string.
    """
    result: dict[str, list] = {}
    input_val: str | None | dict = args.get("custom_replacements")
    if not input_val:
        return result

    # Use parameters from config file
    if args["use_config_file"]:
        if isinstance(input_val, dict):
            return input_val
        else:
            raise Exception(
                f"[{task_id}] Invalid custom_replacements type: expected dict, got {type(result)}"
            )

    parts: list[str] = input_val.split(".")

    for part in parts:
        if not part or ":" not in part:
            influxdb3_local.warn(
                f"[{task_id}] Part '{part}' should contain ':', skipping"
            )
            continue

        name, value_str = part.split(":", 1)
        name = name.strip()
        value_str = value_str.strip()

        # Remove surrounding quotes
        if value_str and value_str[0] == value_str[-1] and value_str[0] in ("'", '"'):
            value_str = value_str[1:-1]

        if "=" not in value_str:
            influxdb3_local.warn(
                f"[{task_id}] Value in part '{part}' does not contain '=', skipping"
            )
            continue

        left, right = value_str.split("=", 1)
        result[name] = [left.strip(), right.strip()]

    if not result:
        raise Exception(
            f"[{task_id}] No valid custom transformations found in '{input_val}'"
        )

    return result


def parse_custom_regex(
    influxdb3_local, args: dict, task_id: str
) -> dict[str, re.Pattern]:
    """
    Parse a custom pattern string into a dictionary of compiled regex patterns or uses value from config file.

    Now `%` matches zero, one, or multiple characters (converted to '.*'),
    and `_` matches exactly one character (converted to '.').

    Input format example:
        'field:"pattern".field2:"pattern2"'

    Args:
        influxdb3_local: InfluxDB client instance for logging.
        args (dict): dict with 'custom_regex' key.
        task_id (str): Identifier used for logging context.

    Returns:
        dict[str, re.Pattern]: A dictionary where:
            - keys are field names or pattern keys
            - values are compiled regex patterns

    Raises:
        Exception: if no valid patterns are found in input_string.
    """
    result: dict[str, re.Pattern] = {}
    input_val: str | None | dict = args.get("custom_regex")
    if not input_val:
        return result

    # Use parameters from config file
    if args["use_config_file"]:
        if isinstance(input_val, dict):
            for key, value in input_val.items():
                regex: re.Pattern | None = build_regex_pattern(
                    influxdb3_local, value, task_id
                )
                if regex:
                    result[key] = regex
            return result
        else:
            raise Exception(
                f"[{task_id}] Invalid custom_regex type: expected dict, got {type(result)}"
            )

    # Split into parts by '.', each part should be like name:"pattern"
    parts: list = input_val.split(".")
    for part in parts:
        if ":" not in part:
            influxdb3_local.warn(
                f"[{task_id}] Part '{part}' does not contain ':', skipping"
            )
            continue

        name, value_str = part.split(":", 1)
        name = name.strip()
        value_str = value_str.strip()

        # Remove surrounding quotes if present
        if (
            len(value_str) >= 2
            and value_str[0] == value_str[-1]
            and value_str[0] in ("'", '"')
        ):
            value_str = value_str[1:-1]

        if not name:
            influxdb3_local.warn(f"[{task_id}] Empty name in part '{part}', skipping")
            continue
        if not value_str:
            influxdb3_local.warn(
                f"[{task_id}] Empty pattern in part '{part}', skipping"
            )
            continue

        compiled: re.Pattern | None = build_regex_pattern(
            influxdb3_local, value_str, task_id
        )
        if not compiled:
            continue

        result[name] = compiled

    if not result:
        raise Exception(f"[{task_id}] No valid regex patterns found in '{input_val}'")

    return result


def build_regex_pattern(
    influxdb3_local, value_str: str, task_id: str
) -> re.Pattern | None:
    # Build regex pattern by iterating characters:
    # - '%' -> '.*'  (zero, one, or many characters)
    # - '_' -> '.'   (exactly one character)
    # - other chars: escape via re.escape
    regex_builder: list = []
    for ch in value_str:
        if ch == "%":
            regex_builder.append(".*")
        elif ch == "_":
            regex_builder.append(".")
        else:
            regex_builder.append(re.escape(ch))
    pattern_str: str = "".join(regex_builder)
    # If you want to match the entire string, anchor with ^ and $:
    # pattern_str = "^" + pattern_str + "$"
    try:
        compiled: re.Pattern = re.compile(pattern_str)
        return compiled
    except re.error as e:
        influxdb3_local.warn(
            f"[{task_id}] Failed to compile regex: {e}, skipping"
        )


def get_fields_names(influxdb3_local, measurement: str, task_id: str) -> list[str]:
    """
    Retrieves the list of field names for a measurement from cache or the database.

    Args:
        influxdb3_local: InfluxDB client instance.
        measurement (str): Name of the measurement to query.
        task_id (str): Unique task identifier.

    Returns:
        list[str]: List of field names.
    """
    # check cache first
    fields: list = influxdb3_local.cache.get(f"{measurement}_fields")
    if fields:
        influxdb3_local.info(f"[{task_id}] Using cached field names for '{measurement}' ({len(fields)} fields)")
        return fields

    influxdb3_local.info(f"[{task_id}] Querying database for field names in '{measurement}'")
    # if not in cache, query the database
    query: str = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = $measurement
        AND data_type != 'Dictionary(Int32, Utf8)'
    """
    res: list[dict] = influxdb3_local.query(query, {"measurement": measurement})

    if not res:
        raise Exception(
            f"[{task_id}] No fields found for measurement '{measurement}'."
        )

    field_names: list[str] = [field["column_name"] for field in res]

    # cache the result for 1 hour
    influxdb3_local.cache.put(f"{measurement}_fields", field_names, 60 * 60)

    return field_names


def get_tag_names(influxdb3_local, measurement: str, task_id: str) -> list[str]:
    """
    Retrieves the list of tag names for a measurement from cache or the database.

    Args:
        influxdb3_local: InfluxDB client instance.
        measurement (str): Name of the measurement to query.
        task_id (str): Unique task identifier.

    Returns:
        list[str]: List of tag names with 'Dictionary(Int32, Utf8)' data type.
    """
    # check cache first
    tags: list = influxdb3_local.cache.get(f"{measurement}_tags")
    if tags:
        influxdb3_local.info(f"[{task_id}] Using cached tag names for '{measurement}' ({len(tags)} tags)")
        return tags

    influxdb3_local.info(f"[{task_id}] Querying database for tag names in '{measurement}'")
    # if not in cache, query the database
    query: str = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = $measurement
        AND data_type = 'Dictionary(Int32, Utf8)'
    """
    res: list[dict] = influxdb3_local.query(query, {"measurement": measurement})

    if not res:
        influxdb3_local.info(
            f"[{task_id}] No tags found for measurement '{measurement}'."
        )
        return []

    tag_names: list[str] = [tag["column_name"] for tag in res]

    # cache the result for 1 hour
    influxdb3_local.cache.put(f"{measurement}_tags", tag_names, 60 * 60)

    return tag_names


def generate_fields_string(tags: list[str], fields: list[str]) -> str:
    """
    Generates a formatted SELECT clause with tags and fields.

    Args:
        tags (list[str]): List of tag names.
        fields (list[str]): List of field names.

    Returns:
        str: Formatted string with quoted field names separated by commas and newlines.
    """
    all_fields: list = tags + fields
    return ",\n\t".join(f'"{field}"' for field in all_fields)


def generate_filter_clause(
    filters: list[tuple[str, str, str | int | float]] | None,
) -> str:
    """
    Generates the WHERE clause for filtering by field/operator/value tuples.

    Args:
        filters (list[tuple[str, str, str | int | float]] | None):
            A list of 3-tuples (field, operator, value) as returned by `parse_field_values`,
            or None.

    Returns:
        str: SQL WHERE clause string combining all filters with AND, or empty string if no filters.
    """
    if not filters:
        return ""

    clauses: list[str] = []
    for field, op, val in filters:
        # Determine whether to quote the value
        if isinstance(val, (int, float)):
            val_str = str(val)
        else:
            # escape single quotes in string values
            escaped = val.replace("'", "''")
            val_str = f"'{escaped}'"

        clauses.append(f'AND\n\t"{field}" {op} {val_str}\n')

    return "".join(clauses)


def generate_query(
    measurement: str,
    filters: list,
    field_names: list[str],
    tag_names: list[str],
    start_time: datetime,
    end_time: datetime,
) -> str:
    """
    Builds an SQL query.

    Args:
        measurement: source measurement name
        tag_names: list of tags
        filters: list of filters
        field_names: list of field names
        start_time: UTC datetime for WHERE time > ...
        end_time:   UTC datetime for WHERE time < ...

    Returns:
        A complete SQL query string.
    """
    # SELECT clause
    fields_clause: str = generate_fields_string(tag_names, field_names)
    filter_clause: str = generate_filter_clause(filters)

    query: str = f"""
            SELECT
                {fields_clause}
            FROM
                '{measurement}'
            WHERE
                time >= '{start_time}'
            AND 
                time < '{end_time}'
            {filter_clause}
            ORDER BY time
        """
    return query


def transform_to_influx_line(
    data: list[dict],
    measurement: str,
    fields_list: list[str],
    tags_list: list,
) -> list[LineBuilder]:
    """
    Transforms data into LineBuilder objects for writing to InfluxDB.

    Args:
        data (list[dict]): List of data rows as dictionaries.
        measurement (str): Name of the target measurement.
        fields_list (list): List of field names.
        tags_list (list): List of tag names.

    Returns:
        list[LineBuilder]: List of LineBuilder objects ready for writing to InfluxDB.
    """
    builders: list = []
    if "time" in fields_list:
        fields_list.remove("time")

    for row in data:
        builder = LineBuilder(measurement)
        timestamp: int = row["time"]
        builder.time_ns(timestamp)

        for tag in tags_list:
            builder.tag(tag, row.get(tag))

        for field_name in fields_list:
            value = row[field_name]
            if isinstance(value, int):
                builder.int64_field(field_name, value)
            elif isinstance(value, float):
                builder.float64_field(field_name, value)
            else:
                builder.string_field(field_name, str(value))

        builders.append(builder)

    return builders


def write_data(
    influxdb3_local,
    data: list,
    target_measurement: str,
    target_database: str | None,
    task_id: str,
):
    """
    Writes data to the target measurement with retry logic.

    Args:
        influxdb3_local: InfluxDB client instance.
        data (list): List of LineBuilder objects to write.
        target_measurement (str): Name of the target measurement.
        target_database (str | None): Target database name, or None to use the default database.
        task_id (str): The task ID.

    Returns:
        tuple[bool, str | None, int]: Tuple containing success status, error message (if any), and number of retries.
    """
    max_retries: int = 3
    retry_count: int = 0
    record_count: int = len(data)
    db_name: str = target_database if target_database else "default"
    log_data: dict = {
        "records": record_count,
        "database": db_name,
        "measurement": target_measurement,
        "max_retries": max_retries,
    }
    influxdb3_local.info(f"[{task_id}] Preparing to write data", log_data)
    try:
        for tries in range(max_retries):
            try:
                for row in data:
                    influxdb3_local.write_to_db(db_name, row)
                success_log: dict = {
                    "records_written": record_count,
                    "database": db_name,
                    "measurement": target_measurement,
                    "retries": retry_count,
                }
                influxdb3_local.info(
                    f"[{task_id}] Successful write to {target_measurement}", success_log
                )
                return True, None, retry_count
            except Exception as e:
                retry_count += 1
                retry_log: dict = {
                    "attempt": tries + 1,
                    "max_retries": max_retries,
                    "records": record_count,
                    "database": db_name,
                    "error": str(e),
                }
                influxdb3_local.warn(
                    f"[{task_id}] Error write attempt {tries + 1}", retry_log
                )
                wait_time: float = (2**tries) + random.random()
                time.sleep(wait_time)
                if tries == max_retries - 1:
                    raise
    except Exception as e:
        failure_log: dict = {
            "records": record_count,
            "database": db_name,
            "measurement": target_measurement,
            "retries": retry_count,
            "error": str(e),
        }
        influxdb3_local.error(f"[{task_id}] Write failed with exception, {failure_log}")
        return False, str(e), retry_count


def _normalize_temp_unit_alias(part: str) -> str | None:
    """
    Given a unit part string (e.g. 'degC', 'celsius', 'K', etc.),
    return a canonical key among 'degC','degF','degK','degR' if recognized,
    or None if not recognized as an absolute temperature unit.
    Comparison is case-insensitive.
    """
    _temp_aliases: dict = {
        "degK": "degK",
        "degR": "degR",
        "degC": "degC",
        "degF": "degF",
        "degc": "degC",
        "celsius": "degC",
        "°c": "degC",
        "degf": "degF",
        "fahrenheit": "degF",
        "°f": "degF",
        "kelvin": "degK",
        "k": "degK",
        "degr": "degR",
        "degrankine": "degR",
        "rankine": "degR",
    }
    key: str = part.lower()
    # Strip possible spaces
    key = key.strip().lower()
    return _temp_aliases.get(key)


def _convert_absolute_temperature(value: float, from_key: str, to_key: str) -> float:
    """
    Convert absolute temperature value from from_key to to_key.
    from_key and to_key are canonical strings: 'degC','degF','degK','degR'.
    Returns the converted float.
    """
    # Step 1: to Kelvin
    if from_key == "degC":
        T_k = value + 273.15
    elif from_key == "degF":
        T_k = (value - 32) * 5.0 / 9.0 + 273.15
    elif from_key == "degK":
        T_k = value
    elif from_key == "degR":
        T_k = value * 5.0 / 9.0
    else:
        raise ValueError(f"Unsupported from-temperature unit '{from_key}'")

    # Step 2: Kelvin to target
    if to_key == "degC":
        return T_k - 273.15
    elif to_key == "degF":
        return (T_k - 273.15) * 9.0 / 5.0 + 32
    elif to_key == "degK":
        return T_k
    elif to_key == "degR":
        return T_k * 9.0 / 5.0
    else:
        raise ValueError(f"Unsupported to-temperature unit '{to_key}'")


def apply_unit_conversion_numeric(
    influxdb3_local, value, transform_name: str, ureg: UnitRegistry, task_id: str
):
    """
    Universal numeric unit conversion according to "convert_<from>_to_<to>".
    Handles absolute temperatures manually (via Kelvin) to avoid Pint's ambiguous offset-unit errors,
    and uses Pint for all other multiplicative units.

    Args:
        influxdb3_local: logger with .warn/.info
        value: int or float
        ureg: Pint unit registry
        transform_name: str of form "convert_<from>_to_<to>"
        task_id: string for logging context

    Returns:
        float: converted magnitude, or original value if conversion fails.
    """
    # 1) Ensure numeric
    if not isinstance(value, (int, float)):
        influxdb3_local.warn(
            f"[{task_id}] Cannot apply unit conversion '{transform_name}' to non-numeric value '{value}'"
        )
        return value

    # 2) Parse transform_name
    m = re.fullmatch(
        r"convert_([^_]+(?:_per_[^_]+)?)_to_([^_]+(?:_per_[^_]+)?)", transform_name
    )
    if not m:
        influxdb3_local.warn(
            f"[{task_id}] Invalid conversion format '{transform_name}'. Expected 'convert_<unit>_to_<unit>'."
        )
        return value

    from_part, to_part = m.group(1), m.group(2)

    # 3) Normalize unit strings for Pint: "_per_" → "/", "_" → " "
    def _normalize_unit_for_pint(part: str) -> str:
        unit = part.replace("_per_", "/")
        unit = unit.replace("_", " ")
        return unit

    from_unit_str: str = _normalize_unit_for_pint(from_part)
    to_unit_str: str = _normalize_unit_for_pint(to_part)

    # 4) Check if both from_part and to_part refer to absolute temperature units
    from_temp_key: str = _normalize_temp_unit_alias(from_part)
    to_temp_key: str = _normalize_temp_unit_alias(to_part)
    if from_temp_key is not None and to_temp_key is not None:
        # Manual absolute temperature conversion
        try:
            return _convert_absolute_temperature(
                float(value), from_temp_key, to_temp_key
            )
        except Exception as e:
            influxdb3_local.warn(
                f"[{task_id}] Temperature conversion from '{from_part}' to '{to_part}' failed for value {value}: {e}"
            )
            return value
    # If one is temperature but the other is not, skip
    if (from_temp_key is not None) ^ (to_temp_key is not None):
        influxdb3_local.warn(
            f"[{task_id}] Cannot convert between temperature unit '{from_part}' and non-temperature '{to_part}'"
        )
        return value

    # 5) For non-temperature units, use Pint
    try:
        quantity = value * ureg(from_unit_str)
    except Exception as e:
        influxdb3_local.warn(
            f"[{task_id}] Failed to interpret {value} as '{from_unit_str}': {e}. Skipping conversion."
        )
        return value

    try:
        q2 = quantity.to(to_unit_str)
        return q2.magnitude
    except Exception as e:
        influxdb3_local.warn(
            f"[{task_id}] Conversion from '{from_unit_str}' to '{to_unit_str}' failed for value {value}: {e}."
        )
        return value


def apply_value_transformation(
    influxdb3_local,
    value,
    transform_name: str,
    field_name: str,
    ureg: UnitRegistry,
    custom_replacements: dict,
    task_id: str,
):
    """
    Apply a single transformation to a value based on transform_name.

    Args:
        influxdb3_local: InfluxDB client for logging.
        value: The original value to transform.
        transform_name (str): Name of the transformation to apply.
        field_name (str): Name of the field (for log context).
        ureg (UnitRegistry): Pint UnitRegistry for unit conversions.
        custom_replacements (dict): Mapping of custom replacement keys to (old, new) tuples.
        task_id (str): Identifier for logging context.

    Returns:
        The transformed value, or the original value if transformation fails or is unknown.
    """
    try:
        transform_func = TRANSFORMATIONS.get(transform_name, None)
        if transform_func:
            value = transform_func(value)
        elif transform_name.startswith("convert_"):
            value = apply_unit_conversion_numeric(
                influxdb3_local, value, transform_name, ureg, task_id
            )
        elif transform_name in custom_replacements:
            old_part, new_part = custom_replacements[transform_name]
            value = value.replace(old_part, new_part)
        else:
            influxdb3_local.warn(
                f"[{task_id}] Unknown transformation '{transform_name}' for field '{field_name}'"
            )
    except Exception as e:
        influxdb3_local.warn(
            f"[{task_id}] Error in transformation '{transform_name}' for field '{field_name}': {str(e)}"
        )

    return value


def apply_name_transformation(
    influxdb3_local,
    name: str,
    transform_name: str,
    custom_replacements: dict,
    task_id: str,
) -> str:
    """
    Apply a single transformation to a name string based on transform_name.

    Args:
        influxdb3_local: InfluxDB client for logging.
        name (str): The original name to transform.
        transform_name (str): Name of the transformation to apply.
        custom_replacements (dict): Mapping of custom replacement keys to (old, new) tuples.
        task_id (str): Identifier for logging context.

    Returns:
        str: The transformed name, or the original if transformation fails or is unknown.
    """
    try:
        transform_func = TRANSFORMATIONS.get(transform_name, None)
        if transform_func:
            name = transform_func(name)
        elif transform_name in custom_replacements:
            old_part, new_part = custom_replacements[transform_name]
            name = name.replace(old_part, new_part)
        else:
            influxdb3_local.warn(
                f"[{task_id}] Unknown transformation '{transform_name}' for '{name}'"
            )
    except Exception as e:
        influxdb3_local.warn(
            f"[{task_id}] Error in transformation '{transform_name}' for '{name}': {str(e)}"
        )

    return name


def process_scheduled_call(
    influxdb3_local, call_time: datetime, args: dict | None = None
):
    """
    Execute a scheduled data transformation and write process for InfluxDB.

    Args:
        influxdb3_local: InfluxDB client or logger-like instance used for querying, writing,
                         and logging (methods: .query, .error, .info, .warn, .write_to_db, etc.).
        call_time (datetime): The current timestamp (UTC). Used to compute the end of the query window.
        args (dict | None): Dictionary of configuration parameters (all values are strings):
            Required keys:
                - "measurement": source measurement name (str).
                - "window": time interval string (e.g. "30d") for how far back to query.
                - "target_measurement": destination measurement name (str).
                - "names_transformations": string defining name transforms (e.g. 'field1:"lower".pattern:"snake"').
                - "values_transformations": string defining value transforms similarly.
            Optional keys:
                - "config_file_path": path to config file to override args (str) .
                - "target_database": target database/bucket name (str).
                - "included_fields": dot-separated field and tag names to include.
                - "excluded_fields": dot-separated field and tag names to exclude.
                - "dry_run": "true"/"false"; if true, only logs transformed results without writing.
                - "custom_replacements": string defining custom replacements per field.
                - "custom_regex": string defining regex-based patterns for transformations.
                - "filters": string defining field filters.
    """
    task_id: str = str(uuid.uuid4())
    influxdb3_local.info(f"[{task_id}] Starting scheduled call with args: {args} and call_time: {call_time}")

    # Override args with config file
    if args:
        if path := args.get("config_file_path", None):
            try:
                plugin_dir_var: str | None = os.getenv("PLUGIN_DIR", None)
                if not plugin_dir_var:
                    influxdb3_local.error(
                        f"[{task_id}] Failed to get PLUGIN_DIR env var"
                    )
                    return
                plugin_dir: Path = Path(plugin_dir_var)
                file_path = plugin_dir / path
                influxdb3_local.info(f"[{task_id}] Reading config file {file_path}")
                with open(file_path, "rb") as f:
                    args = tomllib.load(f)
                    args["use_config_file"] = True
                influxdb3_local.info(f"[{task_id}] New args content: {args}")
            except Exception:
                influxdb3_local.error(f"[{task_id}] Failed to read config file")
                return
        else:
            args["use_config_file"] = False

    required_keys: list = ["measurement", "window", "target_measurement"]

    if not args or any(key not in args for key in required_keys):
        influxdb3_local.error(
            f"[{task_id}] Missing some of the required arguments: {', '.join(required_keys)}"
        )
        return

    try:
        # Set up configuration
        measurement: str = args["measurement"]
        target_measurement: str = args["target_measurement"]
        window: timedelta = parse_time_interval(args["window"], task_id)
        target_database: str | None = args.get("target_database", None)
        included_fields: list = parse_fields(args, "included_fields")
        excluded_fields: list = parse_fields(args, "excluded_fields")
        if included_fields and excluded_fields:
            influxdb3_local.error(
                f"[{task_id}] Both 'included_fields' and 'excluded_fields' arguments are provided. Only one of them can be used."
            )
            return

        dry_run: bool = str(args.get("dry_run", False)).lower() == "true"
        names_transformations: dict = parse_transformation_rules(
            influxdb3_local, args, "names_transformations", task_id
        )
        values_transformations: dict = parse_transformation_rules(
            influxdb3_local, args, "values_transformations", task_id
        )
        if not names_transformations and not values_transformations:
            influxdb3_local.error(f"[{task_id}] No transformation rules provided")
            return
        influxdb3_local.info(f"[{task_id}] Name transformations: {names_transformations}")
        influxdb3_local.info(f"[{task_id}] Value transformations: {values_transformations}")

        custom_replacements: dict = parse_custom_replacements(
            influxdb3_local, args, task_id
        )
        custom_regex: dict = parse_custom_regex(influxdb3_local, args, task_id)
        filters: list = parse_field_filters(influxdb3_local, args, "filters", task_id)

        influxdb3_local.info(f"[{task_id}] Configuration parsed - custom_replacements: {len(custom_replacements)}, custom_regex: {len(custom_regex)}, filters: {len(filters)}")

        # Query InfluxDB
        end_time: datetime = call_time.replace(tzinfo=timezone.utc)
        start_time: datetime = end_time - window
        influxdb3_local.info(f"[{task_id}] Query window: {start_time} to {end_time}")

        # recognize fields and tags to query
        field_names: list[str] = get_fields_names(influxdb3_local, measurement, task_id)
        tag_names: list[str] = get_tag_names(influxdb3_local, measurement, task_id)

        # Filter fields based on included_fields/excluded_fields parameters
        if included_fields:
            fields_to_query: list = [
                field for field in field_names if field in included_fields
            ]
            tags_to_query: list = [
                tag for tag in tag_names if tag in included_fields
            ]
        elif excluded_fields:
            fields_to_query = [
                field for field in field_names if field not in excluded_fields
            ]
            tags_to_query = [
                tag for tag in tag_names if tag not in excluded_fields
            ]
        else:
            fields_to_query = field_names
            tags_to_query = tag_names

        influxdb3_local.info(f"[{task_id}] Fields to query: {fields_to_query}")
        influxdb3_local.info(f"[{task_id}] Tags to query: {tags_to_query}")

        # generate query
        query: str = generate_query(
            measurement, filters, fields_to_query, tags_to_query, start_time, end_time
        )
        influxdb3_local.info(f"[{task_id}] Executing query for {measurement} with {len(filters)} filters")
        results: list = influxdb3_local.query(query)
        influxdb3_local.info(f"[{task_id}] Query executed, {len(results)} records returned")

        if not results:
            influxdb3_local.error(
                f"[{task_id}] No data found from {start_time} to {end_time}"
            )
            return

        ureg: UnitRegistry = UnitRegistry()
        # Apply transformations
        influxdb3_local.info(f"[{task_id}] Starting value transformations on {len(results)} records")
        value_transform_count: int = 0
        for raw in results:
            used_fields: list = []
            for field, transforms in values_transformations.items():
                if field in raw:
                    used_fields.append(field)
                    value = raw[field]
                    for transform_name in transforms:
                        value = apply_value_transformation(
                            influxdb3_local,
                            value,
                            transform_name,
                            field,
                            ureg,
                            custom_replacements,
                            task_id,
                        )
                        value_transform_count += 1
                    raw[field] = value
            for regex_name, transforms in values_transformations.items():
                if regex_name in custom_regex:
                    matched_fields: list = []
                    for field_name in raw.keys():
                        if (
                            custom_regex[regex_name].search(field_name)
                            and field_name not in used_fields
                        ):
                            matched_fields.append(field_name)
                            value = raw[field_name]
                            for transform_name in transforms:
                                value = apply_value_transformation(
                                    influxdb3_local,
                                    value,
                                    transform_name,
                                    field_name,
                                    ureg,
                                    custom_replacements,
                                    task_id,
                                )
                                value_transform_count += 1
                            raw[field_name] = value
                    if matched_fields:
                        influxdb3_local.info(f"[{task_id}] Regex '{regex_name}' matched fields: {matched_fields}")

        if value_transform_count > 0:
            influxdb3_local.info(f"[{task_id}] Applied {value_transform_count} value transformations")
        influxdb3_local.info(f"[{task_id}] Starting name transformations")
        name_transform_count: int = 0
        tags_mapping: dict = {}
        used_tags: list = []
        for tag in tags_to_query:
            new_tag: str = tag
            if tag in names_transformations:
                used_tags.append(tag)
                for transform_name in names_transformations[tag]:
                    new_tag = apply_name_transformation(
                        influxdb3_local,
                        new_tag,
                        transform_name,
                        custom_replacements,
                        task_id,
                    )
                    name_transform_count += 1
            tags_mapping[tag] = new_tag

        for regex_name, transforms in names_transformations.items():
            if regex_name in custom_regex:
                matched_tags: list = []
                for tag_name in tags_to_query:
                    if (
                        custom_regex[regex_name].search(tag_name)
                        and tag_name not in used_tags
                    ):
                        matched_tags.append(tag_name)
                        new_tag: str = tag_name
                        for transform_name in transforms:
                            new_tag = apply_name_transformation(
                                influxdb3_local,
                                new_tag,
                                transform_name,
                                custom_replacements,
                                task_id,
                            )
                            name_transform_count += 1
                        tags_mapping[tag_name] = new_tag
                if matched_tags:
                    influxdb3_local.info(f"[{task_id}] Regex '{regex_name}' matched tags: {matched_tags}")

        fields_mapping: dict = {}
        used_fields: list = []
        for field in fields_to_query:
            new_field: str = field
            if field in names_transformations:
                used_fields.append(field)
                for transform_name in names_transformations[field]:
                    new_field = apply_name_transformation(
                        influxdb3_local,
                        new_field,
                        transform_name,
                        custom_replacements,
                        task_id,
                    )
                    name_transform_count += 1
            fields_mapping[field] = new_field

        for regex_name, transforms in names_transformations.items():
            if regex_name in custom_regex:
                matched_fields: list = []
                for field_name in fields_to_query:
                    if (
                        custom_regex[regex_name].search(field_name)
                        and field_name not in used_fields
                    ):
                        matched_fields.append(field_name)
                        new_field: str = field_name
                        for transform_name in transforms:
                            new_field = apply_name_transformation(
                                influxdb3_local,
                                new_field,
                                transform_name,
                                custom_replacements,
                                task_id,
                            )
                            name_transform_count += 1
                        fields_mapping[field_name] = new_field
                if matched_fields:
                    influxdb3_local.info(f"[{task_id}] Regex '{regex_name}' matched fields: {matched_fields}")

        if name_transform_count > 0:
            influxdb3_local.info(f"[{task_id}] Applied {name_transform_count} name transformations")

        transformed_results: list = []
        all_fields: dict = tags_mapping | fields_mapping
        for raw in results:
            new_point: dict = {}
            for key, value in raw.items():
                new_key = all_fields.get(key, key)
                new_point[new_key] = value
            transformed_results.append(new_point)
        influxdb3_local.info(f"[{task_id}] Data transformation completed")

        if dry_run:
            influxdb3_local.info(
                f"[{task_id}] Dry run is set, transformed results: {transformed_results}"
            )
            return

        # transform data to Line Protocol
        line_protocol_data: list = transform_to_influx_line(
            transformed_results,
            target_measurement,
            list(fields_mapping.values()),
            list(tags_mapping.values()),
        )

        # write data to target database
        success, error, retries = write_data(
            influxdb3_local,
            line_protocol_data,
            target_measurement=target_measurement,
            target_database=target_database,
            task_id=task_id,
        )
        if success:
            influxdb3_local.info(f"[{task_id}] Data written to {target_measurement}")
        else:
            influxdb3_local.error(
                f"[{task_id}] Failed to write data to measurement {target_measurement}: {error}"
            )

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Unexpected error: {e}")


def apply_filters(filters: list, fields: list, tags: list, rows: list):
    if filters:
        filtered_data = []
        for row in rows:
            match = True
            for field_name, op, value in filters:
                compare_fn = _OP_FUNCS[op]
                if not compare_fn(row.get(field_name), value):
                    match = False
                    break
            if match:
                filtered_data.append(row)
    else:
        filtered_data = rows

    for row in filtered_data:
        keys_to_remove = [
            k for k in row if k not in fields and k not in tags and k != "time"
        ]
        for k in keys_to_remove:
            row.pop(k)

    return filtered_data


def process_writes(influxdb3_local, table_batches: list, args: dict | None = None):
    """
    Processes and transforms data batches before writing them to a target measurement.

    args (dict | None): Dictionary of configuration parameters (all values are strings):
    Required keys:
        - "measurement": source measurement name (str).
        - "target_measurement": destination measurement name (str).
        - "names_transformations": string defining name transforms (e.g. 'field1:"lower".pattern:"snake"').
        - "values_transformations": string defining value transforms similarly.
    Optional keys:
        - "config_file_path": path to config file to override args (str) .
        - "target_database": target database/bucket name (str).
        - "included_fields": dot-separated field and tag names to include.
        - "excluded_fields": dot-separated field and tag names to exclude.
        - "dry_run": "true"/"false"; if true, only logs transformed results without writing.
        - "custom_replacements": string defining custom replacements per field.
        - "custom_regex": string defining regex-based patterns for transformations.
        - "filters": string defining field filters.
    """
    task_id: str = str(uuid.uuid4())
    influxdb3_local.info(f"[{task_id}] Starting writes processing")

    # Override args with config file
    if args:
        if path := args.get("config_file_path", None):
            try:
                plugin_dir_var: str | None = os.getenv("PLUGIN_DIR", None)
                influxdb3_local.info(
                    f"[{task_id}] PLUGIN_DIR env var: {plugin_dir_var}"
                )
                if not plugin_dir_var:
                    influxdb3_local.error(
                        f"[{task_id}] Failed to get PLUGIN_DIR env var"
                    )
                    return
                plugin_dir: Path = Path(plugin_dir_var)
                file_path = plugin_dir / path
                influxdb3_local.info(f"[{task_id}] Reading config file {file_path}")
                with open(file_path, "rb") as f:
                    args = tomllib.load(f)
                    args["use_config_file"] = True
                influxdb3_local.info(f"[{task_id}] new args content: {args}")
            except Exception:
                influxdb3_local.error(f"[{task_id}] Failed to read config file")
                return
        else:
            args["use_config_file"] = False

    required_keys: list = ["measurement", "target_measurement"]

    if not args or any(key not in args for key in required_keys):
        influxdb3_local.error(
            f"[{task_id}] Missing some of the required arguments: {', '.join(required_keys)}"
        )
        return

    try:
        # Set up configuration
        measurement: str = args["measurement"]
        target_measurement: str = args["target_measurement"]
        target_database: str | None = args.get("target_database", None)
        included_fields: list = parse_fields(args, "included_fields")
        excluded_fields: list = parse_fields(args, "excluded_fields")
        if included_fields and excluded_fields:
            influxdb3_local.error(
                f"[{task_id}] Both 'included_fields' and 'excluded_fields' arguments are provided. Only one of them can be used."
            )
            return

        dry_run: bool = str(args.get("dry_run", False)).lower() == "true"
        names_transformations: dict = parse_transformation_rules(
            influxdb3_local, args, "names_transformations", task_id
        )
        values_transformations: dict = parse_transformation_rules(
            influxdb3_local, args, "values_transformations", task_id
        )
        if not names_transformations and not values_transformations:
            influxdb3_local.error(f"[{task_id}] No transformation rules provided")
            return
        influxdb3_local.info(f"[{task_id}] Name transformations: {names_transformations}")
        influxdb3_local.info(f"[{task_id}] Value transformations: {values_transformations}")

        custom_replacements: dict = parse_custom_replacements(
            influxdb3_local, args, task_id
        )
        custom_regex: dict = parse_custom_regex(influxdb3_local, args, task_id)
        filters: list = parse_field_filters(influxdb3_local, args, "filters", task_id)

        influxdb3_local.info(f"[{task_id}] Configuration parsed - custom_replacements: {len(custom_replacements)}, custom_regex: {len(custom_regex)}, filters: {len(filters)}")

        # recognize fields and tags to transform and save
        field_names: list[str] = get_fields_names(influxdb3_local, measurement, task_id)
        tag_names: list[str] = get_tag_names(influxdb3_local, measurement, task_id)

        # Filter fields and tags based on included_fields/excluded_fields parameters
        if included_fields:
            fields_to_transform: list = [
                field for field in field_names if field in included_fields
            ]
            tags_to_transform: list = [
                tag for tag in tag_names if tag in included_fields
            ]
        elif excluded_fields:
            fields_to_transform = [
                field for field in field_names if field not in excluded_fields
            ]
            tags_to_transform = [
                tag for tag in tag_names if tag not in excluded_fields
            ]
        else:
            fields_to_transform = field_names
            tags_to_transform = tag_names

        influxdb3_local.info(f"[{task_id}] Fields to transform: {fields_to_transform}")
        influxdb3_local.info(f"[{task_id}] Tags to transform: {tags_to_transform}")

        for table_batch in table_batches:
            table_name: str = table_batch["table_name"]
            if table_name != measurement:
                continue

            influxdb3_local.info(f"[{task_id}] Processing table batch for '{table_name}' with {len(table_batch['rows'])} rows")
            rows: list = apply_filters(
                filters, fields_to_transform, tags_to_transform, table_batch["rows"]
            )
            if not rows:
                influxdb3_local.warn(f"[{task_id}] No data to process after filtering")
                return
            influxdb3_local.info(f"[{task_id}] {len(rows)} rows remaining after filtering")

            ureg: UnitRegistry = UnitRegistry()
            # Apply transformations
            influxdb3_local.info(f"[{task_id}] Starting value transformations on {len(rows)} records")
            value_transform_count: int = 0
            for raw in rows:
                used_fields: list = []
                for field, transforms in values_transformations.items():
                    if field in raw:
                        used_fields.append(field)
                        value = raw[field]
                        for transform_name in transforms:
                            value = apply_value_transformation(
                                influxdb3_local,
                                value,
                                transform_name,
                                field,
                                ureg,
                                custom_replacements,
                                task_id,
                            )
                            value_transform_count += 1
                        raw[field] = value
                for regex_name, transforms in values_transformations.items():
                    if regex_name in custom_regex:
                        matched_fields: list = []
                        for field_name in raw.keys():
                            if (
                                custom_regex[regex_name].search(field_name)
                                and field_name not in used_fields
                            ):
                                matched_fields.append(field_name)
                                value = raw[field_name]
                                for transform_name in transforms:
                                    value = apply_value_transformation(
                                        influxdb3_local,
                                        value,
                                        transform_name,
                                        field_name,
                                        ureg,
                                        custom_replacements,
                                        task_id,
                                    )
                                    value_transform_count += 1
                                raw[field_name] = value
                        if matched_fields:
                            influxdb3_local.info(f"[{task_id}] Regex '{regex_name}' matched fields: {matched_fields}")

            if value_transform_count > 0:
                influxdb3_local.info(f"[{task_id}] Applied {value_transform_count} value transformations")
            influxdb3_local.info(f"[{task_id}] Starting name transformations")
            name_transform_count: int = 0
            tags_mapping: dict = {}
            used_tags: list = []
            for tag in tags_to_transform:
                new_tag: str = tag
                if tag in names_transformations:
                    used_tags.append(tag)
                    for transform_name in names_transformations[tag]:
                        new_tag = apply_name_transformation(
                            influxdb3_local,
                            new_tag,
                            transform_name,
                            custom_replacements,
                            task_id,
                        )
                        name_transform_count += 1
                tags_mapping[tag] = new_tag

            for regex_name, transforms in names_transformations.items():
                if regex_name in custom_regex:
                    matched_tags: list = []
                    for tag_name in tags_to_transform:
                        if (
                            custom_regex[regex_name].search(tag_name)
                            and tag_name not in used_tags
                        ):
                            matched_tags.append(tag_name)
                            new_tag: str = tag_name
                            for transform_name in transforms:
                                new_tag = apply_name_transformation(
                                    influxdb3_local,
                                    new_tag,
                                    transform_name,
                                    custom_replacements,
                                    task_id,
                                )
                                name_transform_count += 1
                            tags_mapping[tag_name] = new_tag
                    if matched_tags:
                        influxdb3_local.info(f"[{task_id}] Regex '{regex_name}' matched tags: {matched_tags}")

            fields_mapping: dict = {}
            used_fields: list = []
            for field in fields_to_transform:
                new_field: str = field
                if field in names_transformations:
                    used_fields.append(field)
                    for transform_name in names_transformations[field]:
                        new_field = apply_name_transformation(
                            influxdb3_local,
                            new_field,
                            transform_name,
                            custom_replacements,
                            task_id,
                        )
                        name_transform_count += 1
                fields_mapping[field] = new_field

            for regex_name, transforms in names_transformations.items():
                if regex_name in custom_regex:
                    matched_fields: list = []
                    for field_name in fields_to_transform:
                        if (
                            custom_regex[regex_name].search(field_name)
                            and field_name not in used_fields
                        ):
                            matched_fields.append(field_name)
                            new_field: str = field_name
                            for transform_name in transforms:
                                new_field = apply_name_transformation(
                                    influxdb3_local,
                                    new_field,
                                    transform_name,
                                    custom_replacements,
                                    task_id,
                                )
                                name_transform_count += 1
                            fields_mapping[field_name] = new_field
                    if matched_fields:
                        influxdb3_local.info(f"[{task_id}] Regex '{regex_name}' matched fields: {matched_fields}")

            if name_transform_count > 0:
                influxdb3_local.info(f"[{task_id}] Applied {name_transform_count} name transformations")

            transformed_results: list = []
            all_fields: dict = tags_mapping | fields_mapping
            for raw in rows:
                new_point: dict = {}
                for key, value in raw.items():
                    new_key = all_fields.get(key, key)
                    new_point[new_key] = value
                transformed_results.append(new_point)
            influxdb3_local.info(f"[{task_id}] Data transformation completed. Total transformed rows: {len(transformed_results)}")

            if dry_run:
                influxdb3_local.info(
                    f"[{task_id}] Dry run is set, transformed results: {transformed_results}"
                )
                return

            # transform data to Line Protocol
            line_protocol_data: list = transform_to_influx_line(
                transformed_results,
                target_measurement,
                list(fields_mapping.values()),
                list(tags_mapping.values()),
            )

            # write data to target database
            success, error, retries = write_data(
                influxdb3_local,
                line_protocol_data,
                target_measurement=target_measurement,
                target_database=target_database,
                task_id=task_id,
            )
            if success:
                influxdb3_local.info(
                    f"[{task_id}] Data written to {target_measurement}"
                )
            else:
                influxdb3_local.error(
                    f"[{task_id}] Failed to write data to measurement {target_measurement}: {error}"
                )

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Unexpected error: {e}")
