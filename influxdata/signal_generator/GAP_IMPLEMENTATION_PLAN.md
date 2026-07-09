# Data Gap Implementation Plan

Implements `GAP_DESIGN.md` (jittered-grid gap schedule, min/max configuration
shape). Read that document first; this plan only sequences the work and maps
it onto files. Where this plan and the design doc disagree, the design doc
wins.

## Files touched

| File | Change |
|------|--------|
| `signal_generator.py` | Gap config parsing, schedule functions, filtering, entry-point wiring, docstring metadata |
| `test_signal_generator.py` | New gap tests; update existing entry-point tests that assert exact output (gaps are on by default) |
| `README.md` | Gap section, headline bullet rewrite, parameter table, troubleshooting entry |
| `manifest.toml` | Version bump `0.2.0` → `0.3.0` |
| `influxdata/library/plugin_library.json` | Description mentions simulated gaps; `last_update` |

## Step 1: Gap config parsing (`signal_generator.py`)

Add defaults next to the existing jitter constants:

```python
DEFAULT_GAP_MIN_DURATION_SECONDS = 10.0
DEFAULT_GAP_MAX_DURATION_SECONDS = 30.0
DEFAULT_GAP_MIN_INTERVAL_SECONDS = 120.0
DEFAULT_GAP_MAX_INTERVAL_SECONDS = 240.0
```

Add `parse_gap_config(args)` following the `parse_jitter_config` contract —
returns `(config, None)` on success or `(None, error_message)` on failure,
error messages prefixed `"Invalid gap config: "`. Config is a tuple:

```python
(enabled, min_duration, max_duration, min_interval, max_interval, pitch, offset, seed)
```

with `pitch = (min_interval + max_interval) / 2` and
`offset = (max_interval - min_interval) / 4` precomputed at parse time.
`seed` is the configured `gap_seed` or `None` (resolved later, Step 3).

Rules (all from the design doc's Validation section):

- `gap_enabled`: accept `"true"`/`"false"` case-insensitive; default `True`;
  anything else is an error. No boolean parser exists yet — add a small
  `_parse_bool` helper.
- All four numeric args parse via `float(raw)` and must be finite
  (`math.isfinite`), matching the existing jitter/pps checks.
- `min_duration >= 0`, `max_duration >= min_duration`, `min_interval > 0`,
  `max_interval >= min_interval`, and the load-bearing rule
  `min_interval >= max_duration`.
- `gap_seed` parses with the same integer coercion used for `jitter_seed`
  (reject bools and non-integral floats).
- Validate every provided argument even when `gap_enabled=false`; disabled
  only skips filtering, not validation.

## Step 2: Schedule and filtering functions (`signal_generator.py`)

Place next to `_rng_for_timestamp` — same primitive, keyed by gap index, with
`"gap"` domain separation:

```python
def _rng_for_gap_index(seed, n):
    payload = f"{seed}:gap:{n}".encode("ascii")
    digest = hashlib.blake2b(payload, digest_size=8).digest()
    return random.Random(int.from_bytes(digest, "big"))
```

`gap_window(gap_config, n)` — draw order is a reproducibility contract:
start offset first, then duration:

```python
rng = _rng_for_gap_index(seed, n)
start = n * pitch + rng.uniform(-offset, +offset)
duration = rng.uniform(min_duration, max_duration)
return (start, start + duration)
```

`in_gap(gap_config, t)` — check candidate indices
`n_hi = int((t + offset) // pitch)` and `n_hi - 1`, skipping `n < 0`;
half-open comparison `start <= t < end`.

`apply_gaps(points, gap_config)` — runs after `apply_timestamp_jitter`, tests
each point's final (jittered) timestamp, returns
`(kept_points, removed_count, gap_window_count)` where `gap_window_count` is
the number of gap indices whose window intersects
`[min(point ts), max(point ts)]` (iterate the index range
`int((t_min - offset - max_duration) // pitch)` to `int((t_max + offset) // pitch)`;
the range is tiny relative to any realistic call window). Memoize
`gap_window` per index in a local dict for the duration of the call so a
window shared by thousands of points hashes once.

## Step 3: Seed resolution and cache (`signal_generator.py`)

```python
GAP_SEED_CACHE_KEY = "signal_gen:gap_seed"
```

`resolve_gap_seed(cache, configured_seed)`:

- configured seed set → return it, never touch the cache;
- else cached value present → return it;
- else generate `random.getrandbits(63)`, `cache.put`, return it.

Called on every scheduled call when gaps are enabled (including first-run
init, so the schedule exists before the first data write). Cache loss simply
regenerates — documented behavior.

`signal_gen:last_time` semantics unchanged: always advances to `now`, even
when every generated point was removed by a gap. No backfill.

## Step 4: Entry-point wiring (`process_scheduled_call`)

Parse order: waveforms → output → pps → jitter → **gap** (new). A gap config
error logs `f"[{task_id}] {gap_error}"` and returns before touching the
cache, matching the invalid-jitter behavior exactly (existing test pattern:
cache stays `None`).

First-run init block: additionally call `resolve_gap_seed` when gaps are
enabled, then return as today.

Generation path, after `apply_timestamp_jitter`:

```python
generated = len(points)
if gap_enabled:
    points, removed, gap_windows = apply_gaps(points, gap_config)
```

Then write as today (an empty kept-list skips the write and still advances
the cache — `write_points` already returns 0 for no lines). Success log
becomes, when gaps removed anything:

```text
Wrote {written} points to {m}.{f} ({generated} generated, {removed} removed by {gap_windows} gap windows)
```

and stays the current one-liner when `removed == 0`, keeping logs quiet in
the common case.

## Step 5: Docstring metadata

Add the six `gap_*` arguments to `scheduled_args_config` (name, example,
description, `"required": false` — same shape as the jitter entries).
Examples: `gap_enabled` → `"false"`, durations → `"10"`/`"30"`, intervals →
`"120"`/`"240"`, `gap_seed` → `"123"`. Descriptions state the min/max bound
guarantees and that gaps are on by default.

## Step 6: Tests (`test_signal_generator.py`)

### Fix existing tests first — gaps are on by default

Any entry-point test asserting exact point counts or timestamps now runs with
an unseeded (random) gap schedule and becomes nondeterministic. Add
`"gap_enabled": "false"` to the args of:

- `test_process_scheduled_call_second_run` (currently passes no args — give
  it `{"gap_enabled": "false"}`)
- `test_process_scheduled_call_custom_pps`
- `test_process_scheduled_call_target_database`
- `test_process_scheduled_call_jitter_zero_keeps_regular_timestamps`
- `test_process_scheduled_call_seeded_jitter_offsets_timestamps`
- `test_process_scheduled_call_seeded_jitter_varies_across_one_point_calls`
- `test_process_scheduled_call_default_jitter_offsets_timestamps`

`test_process_scheduled_call_first_run` and `..._updates_cache` assert only
cache/no-write behavior and can stay as-is; extend `first_run` to also assert
`signal_gen:gap_seed` is populated.

### New parsing/validation tests (mirror the jitter test style)

- defaults (no args → enabled, 10/30/120/240, derived pitch 180 / offset 30)
- explicit values round-trip; `gap_enabled` `"false"`/`"TRUE"`/garbage
- each rejection: non-numeric, `nan`, `inf`, negative min duration,
  `max_duration < min_duration`, `min_interval <= 0`,
  `max_interval < min_interval`, `min_interval < max_duration`,
  non-integer `gap_seed`
- disabled-but-invalid still errors

### New schedule-function tests (design doc Test Plan section)

- **Bounded spacing + non-overlap**: iterate `gap_window` over a few thousand
  indices at defaults and at the equality boundary
  (`min_interval == max_duration`); assert spacing within `[min_i, max_i]`,
  durations within `[min_d, max_d]`, windows disjoint.
- **Membership correctness**: `in_gap` vs brute-force scan over the covering
  index range for a few thousand sampled timestamps.
- **Reproducibility**: same seed → identical windows; different seeds differ.
- **Determinism pin**: one exact-value test in the style of
  `test_apply_timestamp_jitter_seeded_exact_offsets` — compute
  `gap_window(seed=7, n=...)` once after implementation, pin the constants.
  Guards the draw-order and payload-format contract against refactors.

### New entry-point tests

- **Batch invariance**: fixed range, seeded gaps and seeded jitter: one call
  covering 60 s vs 60 calls of 1 s; identical written timestamps. (Choose a
  `gap_seed` whose window lands inside the range so the test is meaningful —
  find it by scanning indices near the chosen epoch time.)
- **Gap spanning call boundaries**: points removed in both adjacent calls.
- **Gap covering entire call window**: zero written, cache still advances,
  info log contains the removed-by-gap count.
- **Catch-up**: init, then a single call far in the future; removed spans
  match the seeded schedule positions.
- **Unseeded stability**: two consecutive calls share the cached auto-seed
  (assert cache key written once and schedule consistent across the calls).
- **Disabled**: `gap_enabled=false` writes all generated points.

## Step 7: README

Per the design doc's Breaking Change section:

- Rewrite the "Gap-filling" headline bullet; add a "Simulated gaps" bullet.
- Add the six parameters to the optional-parameters table.
- Add a "Simulated gaps" section: min/max bound guarantees, on-by-default,
  `gap_enabled=false`, seed behavior, upgrade note (existing triggers pick up
  gaps; deadman/no-data alert demos will fire).
- Troubleshooting entry: "missing spans in query results" → expected default
  behavior, how to disable, how to distinguish from real failures via the
  removed-by-gap log line.
- Output schema / timestamp note: mention missing spans.

## Step 8: Versioning and library metadata

- `manifest.toml`: `version = "0.3.0"` (behavior-changing default).
- `plugin_library.json` signal_generator entry: append simulated-gap mention
  to description, update `last_update`.
- PR notes: call out the default-on behavior change.

## Step 9: Verify

1. `pytest influxdata/signal_generator/test_signal_generator.py` — all pass.
2. Manual smoke: `influxdb3 test` or Docker Compose flow per repo README —
   default trigger for ~10 minutes, query for missing spans near the 3-minute
   cadence; then `gap_enabled=false` run shows continuous data.
3. Grep logs for the removed-by-gap summary line during the smoke run.

## Sequencing

Steps 1–3 are pure additions (safe to land in one commit with their unit
tests). Step 4 flips the default and must land together with the Step 6 test
updates and Steps 5, 7, 8 in the same PR — never ship the wiring without the
README/metadata changes.

## Risks / notes

- **Performance**: 2 blake2b digests per point worst case; memoization in
  `apply_gaps` reduces this to ~2 per gap index per call. Catch-up over days
  of downtime at high `points_per_second` is the worst case and stays linear
  in point count.
- **Float precision**: gap indices near current epoch are ~10^7; `n * pitch`
  products are exact well past 2^53 — no precision concern.
- **Contract freeze**: payload format `f"{seed}:gap:{n}"`, digest size 8, and
  draw order (offset, duration) are frozen once released — changing any of
  them silently reshuffles every seeded schedule. The determinism-pin test
  exists to catch this.
- **Do not** add gap logic to `generate_timestamps` or waveform evaluation;
  filtering happens strictly on final jittered timestamps
  (design doc Pipeline Semantics).
