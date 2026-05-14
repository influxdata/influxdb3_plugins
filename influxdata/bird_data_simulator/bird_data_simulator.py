"""
{
    "plugin_type": ["scheduled"],
    "scheduled_args_config": [
        {
            "name": "bird_count",
            "example": "25",
            "description": "Number of persistent simulated birds to track.",
            "required": false
        },
        {
            "name": "points_per_bird",
            "example": "1",
            "description": "Number of movement points to emit for each bird on each scheduled call, evenly spaced since the previous call.",
            "required": false
        }
    ]
}
"""

import math
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from faker import Faker


MEASUREMENT = "bird_tracking"
CACHE_KEY = "bird_tracking_state"
STATE_VERSION = 1
DEFAULT_BIRD_COUNT = 25
DEFAULT_POINTS_PER_BIRD = 1
MAX_POINTS_PER_CALL = 10000
MILES_PER_DEGREE_LATITUDE = 69.0

BIRD_SPECIES = [
    {
        "common_name": "American Robin",
        "scientific_name": "Turdus migratorius",
        "range": {
            "description": "Most of the contiguous United States",
            "min_lat": 25.0,
            "max_lat": 49.5,
            "min_lon": -124.8,
            "max_lon": -66.9,
        },
        "weight_g": {"min": 77, "max": 85},
        "body_temp_c": {"min": 40.8, "max": 42.4},
        "top_flight_speed_mph": 36,
    },
    {
        "common_name": "Northern Cardinal",
        "scientific_name": "Cardinalis cardinalis",
        "range": {
            "description": "Eastern, central, and south-central United States",
            "min_lat": 25.0,
            "max_lat": 47.5,
            "min_lon": -103.5,
            "max_lon": -67.0,
        },
        "weight_g": {"min": 42, "max": 48},
        "body_temp_c": {"min": 41.0, "max": 42.7},
        "top_flight_speed_mph": 30,
    },
    {
        "common_name": "Bald Eagle",
        "scientific_name": "Haliaeetus leucocephalus",
        "range": {
            "description": "Most of the contiguous United States near water",
            "min_lat": 25.0,
            "max_lat": 49.5,
            "min_lon": -124.8,
            "max_lon": -66.9,
        },
        "weight_g": {"min": 3000, "max": 6300},
        "body_temp_c": {"min": 40.6, "max": 42.2},
        "top_flight_speed_mph": 43,
    },
    {
        "common_name": "Mourning Dove",
        "scientific_name": "Zenaida macroura",
        "range": {
            "description": "Widespread across the contiguous United States",
            "min_lat": 25.0,
            "max_lat": 49.5,
            "min_lon": -124.8,
            "max_lon": -66.9,
        },
        "weight_g": {"min": 86, "max": 170},
        "body_temp_c": {"min": 40.8, "max": 42.5},
        "top_flight_speed_mph": 60,
    },
    {
        "common_name": "Red-tailed Hawk",
        "scientific_name": "Buteo jamaicensis",
        "range": {
            "description": "Open country across the contiguous United States",
            "min_lat": 25.0,
            "max_lat": 49.5,
            "min_lon": -124.8,
            "max_lon": -66.9,
        },
        "weight_g": {"min": 690, "max": 1460},
        "body_temp_c": {"min": 40.6, "max": 42.2},
        "top_flight_speed_mph": 40,
    },
    {
        "common_name": "Canada Goose",
        "scientific_name": "Branta canadensis",
        "range": {
            "description": "Most of the contiguous United States near open water",
            "min_lat": 25.0,
            "max_lat": 49.5,
            "min_lon": -124.8,
            "max_lon": -66.9,
        },
        "weight_g": {"min": 3000, "max": 9000},
        "body_temp_c": {"min": 40.6, "max": 42.0},
        "top_flight_speed_mph": 40,
    },
    {
        "common_name": "Great Blue Heron",
        "scientific_name": "Ardea herodias",
        "range": {
            "description": "Wetlands across the contiguous United States",
            "min_lat": 25.0,
            "max_lat": 49.5,
            "min_lon": -124.8,
            "max_lon": -66.9,
        },
        "weight_g": {"min": 2100, "max": 2500},
        "body_temp_c": {"min": 40.5, "max": 42.0},
        "top_flight_speed_mph": 30,
    },
    {
        "common_name": "Mallard",
        "scientific_name": "Anas platyrhynchos",
        "range": {
            "description": "Wetlands across the contiguous United States",
            "min_lat": 25.0,
            "max_lat": 49.5,
            "min_lon": -124.8,
            "max_lon": -66.9,
        },
        "weight_g": {"min": 1000, "max": 1300},
        "body_temp_c": {"min": 40.7, "max": 42.1},
        "top_flight_speed_mph": 55,
    },
    {
        "common_name": "Blue Jay",
        "scientific_name": "Cyanocitta cristata",
        "range": {
            "description": "Eastern and central United States",
            "min_lat": 25.0,
            "max_lat": 49.5,
            "min_lon": -103.0,
            "max_lon": -67.0,
        },
        "weight_g": {"min": 70, "max": 100},
        "body_temp_c": {"min": 41.0, "max": 42.7},
        "top_flight_speed_mph": 25,
    },
    {
        "common_name": "American Goldfinch",
        "scientific_name": "Spinus tristis",
        "range": {
            "description": "Most of the contiguous United States",
            "min_lat": 28.0,
            "max_lat": 49.5,
            "min_lon": -124.8,
            "max_lon": -66.9,
        },
        "weight_g": {"min": 11, "max": 20},
        "body_temp_c": {"min": 41.2, "max": 43.0},
        "top_flight_speed_mph": 30,
    },
    {
        "common_name": "House Finch",
        "scientific_name": "Haemorhous mexicanus",
        "range": {
            "description": "Urban and open habitats across the contiguous United States",
            "min_lat": 25.0,
            "max_lat": 49.5,
            "min_lon": -124.8,
            "max_lon": -66.9,
        },
        "weight_g": {"min": 16, "max": 27},
        "body_temp_c": {"min": 41.1, "max": 42.9},
        "top_flight_speed_mph": 25,
    },
    {
        "common_name": "Anna's Hummingbird",
        "scientific_name": "Calypte anna",
        "range": {
            "description": "Pacific Coast and inland Southwest",
            "min_lat": 31.0,
            "max_lat": 49.0,
            "min_lon": -124.8,
            "max_lon": -110.0,
        },
        "weight_g": {"min": 3, "max": 6},
        "body_temp_c": {"min": 39.5, "max": 42.8},
        "top_flight_speed_mph": 30,
    },
    {
        "common_name": "Greater Roadrunner",
        "scientific_name": "Geococcyx californianus",
        "range": {
            "description": "Deserts and open country in the Southwest",
            "min_lat": 25.0,
            "max_lat": 38.0,
            "min_lon": -119.5,
            "max_lon": -94.0,
        },
        "weight_g": {"min": 220, "max": 540},
        "body_temp_c": {"min": 40.4, "max": 42.0},
        "top_flight_speed_mph": 20,
    },
    {
        "common_name": "Cactus Wren",
        "scientific_name": "Campylorhynchus brunneicapillus",
        "range": {
            "description": "Southwestern deserts and arid scrub",
            "min_lat": 25.0,
            "max_lat": 37.0,
            "min_lon": -118.5,
            "max_lon": -102.0,
        },
        "weight_g": {"min": 32, "max": 47},
        "body_temp_c": {"min": 41.0, "max": 42.8},
        "top_flight_speed_mph": 20,
    },
    {
        "common_name": "Florida Scrub-Jay",
        "scientific_name": "Aphelocoma coerulescens",
        "range": {
            "description": "Central and coastal Florida scrub",
            "min_lat": 26.5,
            "max_lat": 30.5,
            "min_lon": -82.8,
            "max_lon": -80.0,
        },
        "weight_g": {"min": 66, "max": 92},
        "body_temp_c": {"min": 40.9, "max": 42.5},
        "top_flight_speed_mph": 25,
    },
    {
        "common_name": "California Quail",
        "scientific_name": "Callipepla californica",
        "range": {
            "description": "Pacific Coast, Great Basin edges, and inland western scrub",
            "min_lat": 32.0,
            "max_lat": 49.0,
            "min_lon": -124.8,
            "max_lon": -114.0,
        },
        "weight_g": {"min": 140, "max": 230},
        "body_temp_c": {"min": 40.6, "max": 42.2},
        "top_flight_speed_mph": 38,
    },
    {
        "common_name": "Wild Turkey",
        "scientific_name": "Meleagris gallopavo",
        "range": {
            "description": "Forests and openings across much of the contiguous United States",
            "min_lat": 25.0,
            "max_lat": 49.5,
            "min_lon": -124.8,
            "max_lon": -66.9,
        },
        "weight_g": {"min": 2500, "max": 10800},
        "body_temp_c": {"min": 40.6, "max": 42.1},
        "top_flight_speed_mph": 55,
    },
    {
        "common_name": "Sandhill Crane",
        "scientific_name": "Antigone canadensis",
        "range": {
            "description": "Wetlands, grasslands, and migration corridors in the United States",
            "min_lat": 26.0,
            "max_lat": 49.5,
            "min_lon": -123.5,
            "max_lon": -75.0,
        },
        "weight_g": {"min": 3200, "max": 5200},
        "body_temp_c": {"min": 40.5, "max": 42.0},
        "top_flight_speed_mph": 50,
    },
    {
        "common_name": "Steller's Jay",
        "scientific_name": "Cyanocitta stelleri",
        "range": {
            "description": "Western forests and mountain regions",
            "min_lat": 31.0,
            "max_lat": 49.5,
            "min_lon": -124.8,
            "max_lon": -105.0,
        },
        "weight_g": {"min": 100, "max": 140},
        "body_temp_c": {"min": 40.9, "max": 42.5},
        "top_flight_speed_mph": 25,
    },
    {
        "common_name": "Wood Stork",
        "scientific_name": "Mycteria americana",
        "range": {
            "description": "Florida, Gulf Coast, and southeastern coastal wetlands",
            "min_lat": 25.0,
            "max_lat": 34.0,
            "min_lon": -98.0,
            "max_lon": -79.0,
        },
        "weight_g": {"min": 2000, "max": 3300},
        "body_temp_c": {"min": 40.5, "max": 42.0},
        "top_flight_speed_mph": 25,
    },
]

def process_scheduled_call(
    influxdb3_local, call_time: datetime, args: Optional[Dict[str, Any]] = None
) -> None:
    """Generate simulated bird tracking points on a schedule."""
    task_id = str(uuid.uuid4())
    args = args or {}

    try:
        bird_count = _positive_int(args.get("bird_count"), DEFAULT_BIRD_COUNT)
        points_per_bird = _positive_int(
            args.get("points_per_bird"), DEFAULT_POINTS_PER_BIRD
        )
        total_points = bird_count * points_per_bird

        if total_points > MAX_POINTS_PER_CALL:
            raise ValueError(
                "bird_count * points_per_bird must be less than or equal to "
                f"{MAX_POINTS_PER_CALL}; got {total_points}"
            )

        rng = random.Random(time.time_ns())
        faker = _build_faker(rng)
        state = _load_state(influxdb3_local, bird_count, rng, faker, task_id)

        call_time_ns = _datetime_to_ns(call_time)
        step_seconds = _calculate_step_seconds(
            state.get("last_call_time_ns"), call_time_ns, points_per_bird
        )
        first_point_time_ns = call_time_ns - int(
            (points_per_bird - 1) * step_seconds * 1_000_000_000
        )

        written = 0
        for point_index in range(points_per_bird):
            timestamp_ns = first_point_time_ns + int(
                point_index * step_seconds * 1_000_000_000
            )

            for bird in state["flock"]:
                _advance_bird(bird, step_seconds, rng)
                influxdb3_local.write(_build_tracking_line(bird, timestamp_ns))
                written += 1

        state["last_call_time_ns"] = call_time_ns
        influxdb3_local.cache.put(CACHE_KEY, state)
        influxdb3_local.info(
            f"[{task_id}] bird_tracking wrote {written} point(s) "
            f"for {bird_count} persistent bird(s)"
        )

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] bird_tracking failed: {e}")
        raise


def _load_state(
    influxdb3_local,
    bird_count: int,
    rng: random.Random,
    faker: Faker,
    task_id: str,
) -> Dict[str, Any]:
    cached_state = influxdb3_local.cache.get(CACHE_KEY, default=None)

    if _is_valid_state(cached_state, bird_count):
        return cached_state

    state = _new_state(bird_count, rng, faker)
    influxdb3_local.cache.put(CACHE_KEY, state)
    influxdb3_local.info(
        f"[{task_id}] Initialized a flock of {bird_count} simulated bird(s) "
        f"from {len(BIRD_SPECIES)} embedded species"
    )
    return state


def _is_valid_state(state: Any, bird_count: int) -> bool:
    if not isinstance(state, dict):
        return False

    if state.get("version") != STATE_VERSION:
        return False

    if state.get("bird_count") != bird_count:
        return False

    flock = state.get("flock")
    return isinstance(flock, list) and len(flock) == bird_count


def _new_state(
    bird_count: int, rng: random.Random, faker: Faker
) -> Dict[str, Any]:
    species_catalog = BIRD_SPECIES
    _validate_species_catalog(species_catalog)

    return {
        "version": STATE_VERSION,
        "bird_count": bird_count,
        "flock": [
            _create_bird(index, species_catalog, rng, faker)
            for index in range(bird_count)
        ],
        "last_call_time_ns": None,
    }


def _validate_species_catalog(species: Any) -> None:
    if not isinstance(species, list) or not species:
        raise ValueError("species metadata must be a non-empty list")

    required_keys = {
        "common_name",
        "range",
        "weight_g",
        "body_temp_c",
        "top_flight_speed_mph",
    }
    range_keys = {"min_lat", "max_lat", "min_lon", "max_lon"}

    for item in species:
        missing = required_keys - set(item)
        if missing:
            raise ValueError(f"missing species keys: {sorted(missing)}")

        bird_range = item["range"]
        missing_range = range_keys - set(bird_range)
        if missing_range:
            raise ValueError(
                f"{item['common_name']} missing range keys: {sorted(missing_range)}"
            )


def _create_bird(
    index: int,
    species_catalog: List[Dict[str, Any]],
    rng: random.Random,
    faker: Faker,
) -> Dict[str, Any]:
    species = rng.choice(species_catalog)
    bird_range = species["range"]

    return {
        "index": index,
        "species": species,
        "name": _first_name(faker),
        "latitude": rng.uniform(bird_range["min_lat"], bird_range["max_lat"]),
        "longitude": rng.uniform(bird_range["min_lon"], bird_range["max_lon"]),
        "heading": rng.uniform(0.0, 360.0),
        "speed": 0.0,
        "body_temp_baseline": rng.uniform(
            species["body_temp_c"]["min"], species["body_temp_c"]["max"]
        ),
        "body_temp": 0.0,
        "phase": rng.uniform(0.0, math.tau),
        "phase_step": rng.uniform(0.05, 0.18),
        "movement_tick": 0,
    }


def _advance_bird(
    bird: Dict[str, Any], elapsed_seconds: float, rng: random.Random
) -> None:
    species = bird["species"]
    top_speed = float(species["top_flight_speed_mph"])

    bird["movement_tick"] += 1
    wave = (
        math.sin(bird["phase"] + (bird["movement_tick"] * bird["phase_step"])) + 1.0
    ) / 2.0
    noise = rng.gauss(0.0, 0.08)
    speed = top_speed * max(0.0, min(1.0, wave + noise))

    if rng.random() < 0.04:
        speed *= rng.uniform(0.0, 0.15)

    bird["speed"] = max(0.0, min(top_speed, speed))
    bird["heading"] = (bird["heading"] + rng.gauss(0.0, 8.0)) % 360.0
    _advance_position(bird, elapsed_seconds)
    bird["body_temp"] = _body_temperature(bird, rng)


def _advance_position(bird: Dict[str, Any], elapsed_seconds: float) -> None:
    distance_miles = bird["speed"] * elapsed_seconds / 3600.0
    heading_radians = math.radians(bird["heading"])

    delta_lat = math.cos(heading_radians) * distance_miles
    delta_lat /= MILES_PER_DEGREE_LATITUDE

    miles_per_degree_lon = MILES_PER_DEGREE_LATITUDE * max(
        0.2, math.cos(math.radians(bird["latitude"]))
    )
    delta_lon = math.sin(heading_radians) * distance_miles
    delta_lon /= miles_per_degree_lon

    bird["latitude"] += delta_lat
    bird["longitude"] += delta_lon
    _keep_bird_inside_range(bird)


def _keep_bird_inside_range(bird: Dict[str, Any]) -> None:
    bird_range = bird["species"]["range"]

    if bird["latitude"] < bird_range["min_lat"]:
        bird["latitude"] = bird_range["min_lat"]
        bird["heading"] = (180.0 - bird["heading"]) % 360.0
    elif bird["latitude"] > bird_range["max_lat"]:
        bird["latitude"] = bird_range["max_lat"]
        bird["heading"] = (180.0 - bird["heading"]) % 360.0

    if bird["longitude"] < bird_range["min_lon"]:
        bird["longitude"] = bird_range["min_lon"]
        bird["heading"] = (-bird["heading"]) % 360.0
    elif bird["longitude"] > bird_range["max_lon"]:
        bird["longitude"] = bird_range["max_lon"]
        bird["heading"] = (-bird["heading"]) % 360.0


def _body_temperature(bird: Dict[str, Any], rng: random.Random) -> float:
    species_temp = bird["species"]["body_temp_c"]
    jittered = bird["body_temp_baseline"] + rng.gauss(0.0, 0.08)
    return max(species_temp["min"], min(species_temp["max"], jittered))


def _build_tracking_line(bird: Dict[str, Any], timestamp_ns: int):
    line = LineBuilder(MEASUREMENT)
    line.tag("species", bird["species"]["common_name"])
    line.tag("name", bird["name"])
    line.float64_field("body_temp", round(bird["body_temp"], 3))
    line.float64_field("longitude", round(bird["longitude"], 6))
    line.float64_field("latitude", round(bird["latitude"], 6))
    line.float64_field("speed", round(bird["speed"], 3))
    line.float64_field("heading", round(bird["heading"], 3))
    line.time_ns(timestamp_ns)
    return line


def _calculate_step_seconds(
    last_call_time_ns: Optional[int], call_time_ns: int, points_per_bird: int
) -> float:
    if last_call_time_ns is None:
        return 1.0

    elapsed = max(0.000001, (call_time_ns - last_call_time_ns) / 1_000_000_000)
    return elapsed / points_per_bird


def _first_name(faker: Faker) -> str:
    return faker.first_name()


def _build_faker(rng: random.Random):
    faker = Faker()
    faker.seed_instance(rng.randint(0, 2**32 - 1))
    return faker


def _datetime_to_ns(value: Any) -> int:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return int(value.timestamp() * 1_000_000_000)

    if isinstance(value, (int, float)):
        return int(value)

    return time.time_ns()


def _positive_int(value: Any, default: int) -> int:
    if value is None or value == "":
        return default

    parsed = int(value)
    if parsed < 1:
        raise ValueError("bird_count and points_per_bird values must both be at least 1")

    return parsed
