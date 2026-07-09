# Data Gap Design

## Purpose

Add simulated temporary disconnections to the signal generator so generated data
contains realistic missing spans. A gap represents a data source outage: points
inside the outage window are absent from output.

Gaps are not a waveform type. They affect whether points are written, not the
signal value calculation.

## Important Context

This design depends on existing Signal Generator behavior documented in:

- `README.md`: The plugin currently advertises gap-filling behavior, meaning it
  generates continuous data between scheduled executions. Simulated gaps are an
  intentional exception to that behavior and must be documented as such (see
  [Breaking Change and Documentation Impact](#breaking-change-and-documentation-impact)).
- `README.md`: The first scheduled execution initializes cache state and writes
  no data. Gap scheduling preserves this startup behavior.
- `README.md`: `points_per_second` controls generated timestamp resolution.
  Gap behavior must work equally well when each scheduled call writes many
  points or only one point.
- `README.md` (Timestamp jitter section): Timestamp jitter is configured as
  flat trigger arguments, is enabled by default, and uses a seed for
  reproducibility. Gap configuration follows the same style. Jitter applies
  after nominal timestamp generation; simulated gaps apply after jitter so
  filtering uses final recorded timestamps.
- `signal_generator.py`: Cache state currently stores `signal_gen:last_time` as
  the unjittered generation boundary. Gaps must not change that meaning.
- `signal_generator.py`: `_rng_for_timestamp` already derives per-timestamp
  jitter from a `blake2b` digest of `(seed, timestamp)`, so draws are a pure
  function of their inputs. The gap schedule uses the same primitive, keyed by
  gap index instead of timestamp.
- `signal_generator.py`: Trigger arguments are parsed at scheduled-call runtime,
  so invalid gap configuration must be reported at runtime and should produce no
  writes for that call.
- `signal_generator.py`: Waveforms are composed independently from output
  configuration. Gaps stay outside waveform configuration and are not a
  waveform type.

## User Model

Users configure explicit bounds on how long outages last and how often they
begin. Both are randomized within those bounds, so output does not look
mechanically periodic.

Gaps are enabled by default. This is a breaking change for existing triggers;
see [Breaking Change and Documentation Impact](#breaking-change-and-documentation-impact)
for the required mitigations. Users who need continuous output can disable
gaps with `gap_enabled=false`.

## Configuration Shape

Gaps are configured with flat trigger arguments:

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `gap_enabled` | boolean string | `true` | Enables or disables simulated gaps. |
| `gap_min_duration_seconds` | float string | `10.0` | Shortest outage duration, in seconds. |
| `gap_max_duration_seconds` | float string | `30.0` | Longest outage duration, in seconds. |
| `gap_min_interval_seconds` | float string | `120.0` | Shortest time between consecutive gap starts, in seconds. |
| `gap_max_interval_seconds` | float string | `240.0` | Longest time between consecutive gap starts, in seconds. |
| `gap_seed` | integer string | none | Optional seed for reproducible gap timing. |

Example:

```text
gap_min_duration_seconds=10,gap_max_duration_seconds=30,gap_min_interval_seconds=120,gap_max_interval_seconds=240,gap_seed=123
```

The min/max bounds are hard guarantees, not soft targets:

- every gap's duration lies in
  `[gap_min_duration_seconds, gap_max_duration_seconds]`;
- the time between consecutive gap starts lies in
  `[gap_min_interval_seconds, gap_max_interval_seconds]`.

Internally the schedule derives a grid pitch and a per-gap start offset from
the interval bounds:

```text
pitch  = (gap_min_interval_seconds + gap_max_interval_seconds) / 2
offset = (gap_max_interval_seconds - gap_min_interval_seconds) / 4

start(n)    = n * pitch + uniform(-offset, +offset)
duration(n) = uniform(gap_min_duration_seconds, gap_max_duration_seconds)
```

The offset half-range is a quarter of the interval span, not half, because the
spacing between consecutive starts is `pitch` plus the *difference of two
independent offsets*: that difference spans twice the offset half-range, so a
quarter-span offset makes consecutive spacing land exactly in
`[gap_min_interval_seconds, gap_max_interval_seconds]`. See
[Gap Schedule Algorithm](#gap-schedule-algorithm).

Out-of-range values are rejected by validation and logged as errors; they are
never silently corrected or clamped.

## Default Behavior

Default gaps should be visible but not dominant:

```text
gap_min_duration_seconds = 10.0
gap_max_duration_seconds = 30.0
gap_min_interval_seconds = 120.0
gap_max_interval_seconds = 240.0
```

This yields outage durations from 10 to 30 seconds, and gap starts spaced 2 to
4 minutes apart (usually close to 3 minutes). Expected data loss is
`mean duration / mean interval` = 20/180 ≈ 11% of points, and at least
`gap_min_interval_seconds - gap_max_duration_seconds` = 90 seconds of
continuous data are guaranteed between consecutive gaps. At the default
`points_per_second=1.0`, a typical gap removes about 20 points.

The defaults are intentionally moderate: users should notice missing spans in
query results and dashboards without losing most generated data.

## Disabling Gaps

Set `gap_enabled=false` to preserve continuous output. This is the only
supported disable mechanism.

Setting both duration bounds to `0` is valid and produces empty gap windows
(effectively no gaps), but `gap_enabled=false` is the explicit, documented
form.

## Gap Schedule Algorithm

The gap schedule is a **jittered grid**: absolute time is divided into strata
of width `pitch`, anchored at Unix epoch 0, and stratum `n` contains exactly
one gap whose start offset and duration are derived from a hash of
`(seed, n)`. Every gap window is a pure function of
`(gap_seed, configuration, gap_index)` — no sequential state, no replay from
an anchor.

This is the same construction the plugin already uses for per-timestamp jitter
(`_rng_for_timestamp`), keyed by gap index instead of timestamp:

```python
def _rng_for_gap_index(seed, n):
    payload = f"{seed}:gap:{n}".encode("ascii")
    digest = hashlib.blake2b(payload, digest_size=8).digest()
    return random.Random(int.from_bytes(digest, "big"))

def gap_window(seed, n):
    """Gap n's half-open [start, end) window, in epoch seconds.
    Draw order is fixed: start offset first, then duration."""
    rng = _rng_for_gap_index(seed, n)
    start = n * pitch + rng.uniform(-offset, +offset)
    duration = rng.uniform(min_duration, max_duration)
    return (start, start + duration)
```

The `"gap"` tag in the payload domain-separates gap draws from any other
hash-derived draws using the same numeric key space. The draw order (offset,
then duration) is part of the reproducibility contract: reordering the draws
changes every schedule produced by a given seed.

### Membership test

Filtering asks, per point: is final timestamp `t` inside any gap window? Under
the validation constraint below, at most two gap indices can contain `t`, so
membership is O(1):

```python
def in_gap(seed, t):
    n_hi = int((t + offset) // pitch)
    for n in (n_hi, n_hi - 1):
        if n < 0:
            continue
        start, end = gap_window(seed, n)
        if start <= t < end:
            return True
    return False
```

### Properties

These follow from the construction and were verified by simulation over 300k
gap windows and 200k random membership queries against brute force:

- **Batch-size invariance**: membership depends only on the point's final
  timestamp, never on scheduled-call boundaries or how many points a call
  generates. Filtering one point per call and filtering thousands per call
  produce identical output. In particular, a trigger generating one point per
  execution does *not* roll a fresh gap decision each execution — the known
  failure mode of per-call probability draws, where outage frequency scales
  with trigger frequency instead of simulated time.
- **Reproducibility**: same seed and configuration produce bit-identical gap
  windows, across calls, restarts, and batch sizes.
- **No persistent state**: nothing about the schedule is stored. The only new
  cache entry is the auto-generated seed when `gap_seed` is omitted (see
  [Cache Semantics](#cache-semantics)).
- **Bounded spacing**: consecutive gap starts are spaced
  `pitch + (offset_next - offset_prev)`, and each offset is at most a quarter
  of the interval span, so spacing always lies in
  `[gap_min_interval_seconds, gap_max_interval_seconds]` — the bounds the
  argument names promise. Verified: 300k consecutive spacings at defaults all
  fell within [120, 240], mean 180.
- **Non-overlap**: gaps cannot overlap when the validation constraint
  `gap_min_interval_seconds >= gap_max_duration_seconds` holds, and at least
  `gap_min_interval_seconds - gap_max_duration_seconds` seconds of data are
  guaranteed between consecutive gaps (90 seconds at defaults).
- **Spacing distribution**: within its [min, max] bounds, spacing follows a
  triangular distribution peaked at the midpoint, not a uniform one, and every
  gap stays within `±offset` of its own grid position, so there is no
  cumulative drift — the schedule is quasi-periodic rather than a renewal
  process with independent inter-arrival times. The min/max bounds are exact;
  only the shape inside them is not uniform. This trade-off is deliberate; see
  [Alternative Schedule Algorithms Considered](#alternative-schedule-algorithms-considered).
- **Configuration is part of the schedule identity**: changing any interval or
  duration bound re-derives the entire schedule. An in-progress gap does not
  survive a configuration change.

### Prior art

This lattice-plus-keyed-hash construction is standard for reproducible,
random-access randomness over an unbounded domain: jittered stratified
sampling (one sample per stratum, offset from the stratum anchor), stateless
procedural world generation (cell coordinates hashed to a per-cell seed, with
a proven bounded influence radius that makes constant-neighborhood queries
correct), Pixar's correlated multi-jittered sampling (any sample computable
directly from its index), and counter-based RNGs such as Philox (the Nth draw
is a stateless function of key and counter). Chaos-testing tools document the
anti-patterns this design avoids: per-request probability draws couple fault
frequency to request frequency, and re-seeding a sequential RNG per evaluation
collapses to a constant decision.

## Pipeline Semantics

Gaps are applied after signal generation and timestamp jitter:

```text
nominal timestamps -> signal values -> jittered timestamps -> gap filtering -> write
```

This ordering matters. Jitter determines the final recorded timestamp. Gap
filtering then removes any point whose final timestamp lands inside a simulated
outage window.

The result is a source-disconnection model: points inside the outage do not
exist in output.

## Gap Window Semantics

A gap is a half-open time window:

```text
[gap_start, gap_end)
```

Any point with a final timestamp inside that window is removed. Points exactly
at `gap_end` are kept. This mirrors the existing timestamp generation convention
that avoids duplicate boundary handling across adjacent windows.

The same seed controls both the start-offset and duration draws, preserving a
single reproducibility knob.

Example, at defaults with some seed (`pitch` = 180, `offset` = 30; gap indices
are large in practice because the grid is anchored at epoch — small indices
shown for readability):

```text
gap_min_interval_seconds = 120
gap_max_interval_seconds = 240
gap_min_duration_seconds = 10
gap_max_duration_seconds = 30

gap n   nominal start n*180   actual start   duration
1000    180000                179973         27
1001    180180                180198         13
1002    180360                180351         22
```

Nominal positions are a fixed grid; each gap's start shifts by at most
`offset` around its own grid position. Consecutive starts here are spaced
225s and 153s apart — always within [120, 240].

## Randomness

Gap randomness is based on absolute simulated time, not on write-cycle
boundaries.

This is required for low-volume configurations such as one point per scheduled
call. A per-call random decision would make gaps correlate with trigger
executions and could produce "all or nothing" behavior each cycle.
Absolute-time gap windows produce the same outage pattern whether points are
written one at a time or batched many per call.

When `gap_seed` is set, the gap schedule is reproducible: the same seed and
configuration produce the same gap windows regardless of scheduled-call batch
size, restarts, or catch-up after downtime.

When `gap_seed` is omitted, the plugin generates a random integer seed during
the first-run initialization and stores it in the cache (see
[Cache Semantics](#cache-semantics)). All subsequent calls read the cached
seed and take the same code path as an explicit seed, so an outage that begins
in one execution continues coherently in later executions until its duration
ends. If the cache is lost (for example, across a server restart that clears
the trigger cache), a new seed is generated and the schedule changes: an
in-progress gap may end early or new gaps may appear at different times. This
is acceptable for demo data; users who need schedule stability across restarts
should set `gap_seed` explicitly.

## Cache Semantics

The cache continues to represent generated-time progress, not written-time
progress. `signal_gen:last_time` keeps its existing meaning: the unjittered
generation boundary.

One new cache key is added:

- `signal_gen:gap_seed`: the auto-generated gap seed, written once during
  first-run initialization when `gap_seed` is not configured. Never written
  when `gap_seed` is configured.

When points are removed by gaps, the plugin still advances its generation
boundary. Missing data must remain missing; it must not be backfilled during
the next scheduled call.

Catch-up after downtime follows the same rule: when a call generates a long
range of points (because `last_time` is far in the past), the absolute-time
gap schedule applies to that entire range, so backfilled spans contain the
same gaps they would have contained if the plugin had been running.

## Validation

Valid gap configuration:

- `gap_enabled` parses as a boolean when provided.
- `gap_min_duration_seconds` parses as a finite float with value `>= 0`.
- `gap_max_duration_seconds` parses as a finite float with value
  `>= gap_min_duration_seconds`.
- `gap_min_interval_seconds` parses as a finite float with value `> 0`.
- `gap_max_interval_seconds` parses as a finite float with value
  `>= gap_min_interval_seconds`.
- `gap_min_interval_seconds >= gap_max_duration_seconds`.
- `gap_seed` is omitted or parses as an integer.

Finiteness matters: `inf` passes a bare `>= 0` check and would produce a
permanent outage. The existing `points_per_second` and jitter validation
already require finite values; gap validation matches.

`gap_min_interval_seconds >= gap_max_duration_seconds` is the load-bearing
rule, and reads as its own explanation: the shortest time between two outage
starts must fit the longest outage. It guarantees:

- gap windows never overlap (worst case: gap `n` ends at
  `n*pitch + offset + max_duration`, gap `n+1` starts at
  `(n+1)*pitch - offset`, and the difference is
  `pitch - 2*offset - max_duration = min_interval - max_duration >= 0`);
- the membership test only ever needs to inspect two candidate indices;
- a minimum of `gap_min_interval_seconds - gap_max_duration_seconds` seconds
  of continuous data between consecutive gaps.

Defaults satisfy it: `120 >= 30`. At exact equality gaps may touch
back-to-back (the half-open windows still never overlap or double-remove a
boundary point); simulation at the equality boundary over 300k gaps confirmed
no overlap.

Invalid configuration logs an error and writes nothing for that scheduled
call. Runtime validation is the enforcement point because trigger arguments
are not validated at trigger-creation time. Values are rejected, not clamped.

## Interaction With Jitter

Gaps operate on jittered timestamps. A point near a gap boundary can be moved
into or out of the gap by timestamp jitter.

This is desirable: from the user's perspective, gaps describe missing output
time ranges, and jitter describes the final recorded timestamp. Filtering after
jitter makes the final written timeline match the configured outage windows.

`gap_seed` and `jitter_seed` are independent knobs, and the gap draws are
domain-separated from jitter draws by the `"gap"` payload tag, so identical
seed values cannot produce correlated draws.

## Interaction With Waveforms

Waveforms still define what values would have existed at each generated time.
Gaps only remove points from output.

Deterministic waveforms remain anchored to absolute time. Stochastic waveforms
continue to use their own seed behavior. Gap configuration should not alter
waveform values outside the missing windows.

## Observability

Normal successful runs should report how many points were written. When gaps
remove points, logs should make that visible without becoming noisy.

Useful summary fields:

- generated point count
- removed-by-gap point count
- written point count
- number of gap windows intersecting the call window

This helps users distinguish "no points because interval too short" from
"points generated but all removed by simulated gap."

## Breaking Change and Documentation Impact

Enabling gaps by default changes the plugin's advertised core behavior.
Shipping this requires, in the same change:

- Rewrite the README "Gap-filling" headline bullet: continuous generation
  between executions still holds, but simulated outage gaps are now present by
  default and are a feature, not a bug.
- Add a README troubleshooting entry for "missing spans in query results"
  explaining simulated gaps and `gap_enabled=false`.
- Call out explicitly that existing triggers pick up default gaps on upgrade
  with no configuration change, and that downstream demos using deadman or
  no-data alerts will start firing on the simulated outages. That may be
  desirable (it exercises those alerts) but must not be a surprise.
- Add the six `gap_*` arguments to the plugin docstring's
  `scheduled_args_config` metadata (required for InfluxDB 3 Explorer UI
  integration).
- Update `influxdata/library/plugin_library.json` metadata for the new
  version.
- Note the behavior change in the release/PR notes.

## Test Plan

Beyond unit tests for parsing and validation (each rule above, valid and
invalid, including `inf`/`nan`, swapped min/max, and the
`min_interval >= max_duration` rule):

- **Batch invariance**: generate a fixed time range one point per call vs.
  all points in one call; written timestamps must be identical.
- **Membership correctness**: compare `in_gap` against a brute-force scan of
  all gap windows over a large sampled range.
- **Bounded spacing**: assert consecutive gap starts are always spaced within
  `[gap_min_interval_seconds, gap_max_interval_seconds]` and durations within
  `[gap_min_duration_seconds, gap_max_duration_seconds]` over a large index
  range.
- **Non-overlap**: assert windows are disjoint over a large index range at
  the validation boundary (`gap_min_interval_seconds ==
  gap_max_duration_seconds`) and at defaults.
- **Reproducibility**: same seed and config produce identical windows across
  separate processes; different seeds produce different windows.
- **Gap spanning call boundaries**: a gap that starts in one call's window
  and ends in a later call's window removes points in both calls.
- **Gap covering an entire call window**: call writes zero points, logs the
  removed-by-gap count, and still advances `last_time`.
- **Catch-up**: after simulated downtime, the backfilled range contains gaps
  at the same absolute positions as an uninterrupted run with the same seed.
- **Unseeded stability**: with `gap_seed` omitted, the cached auto-seed makes
  consecutive calls share one schedule.
- **Disabled**: `gap_enabled=false` writes all generated points.
- **First-run init**: unchanged — no writes, cache initialized (including
  `signal_gen:gap_seed` when unseeded).

## Alternative Schedule Algorithms Considered

### Sequential renewal process

Draw each inter-gap interval from
`uniform(gap_min_interval_seconds, gap_max_interval_seconds)` and accumulate:
gap `n+1` starts where gap `n`'s interval ends. This gives independent,
identically distributed (uniform) spacing — but start times are cumulative
sums, so answering "is timestamp T inside a gap?" requires replaying every
draw from an anchor (millions of draws per call for an epoch anchor) or
persisting RNG state in the cache. Persisted state breaks batch invariance and
catch-up determinism, and grows the cache contract. Renewal processes do not
admit O(1) random access; this disqualifies them here. The jittered grid keeps
the same [min, max] spacing bounds and mean, trading uniform spacing for a
triangular distribution inside the bounds.

### Per-call probability draw

Each scheduled call rolls "does an outage start now?". Rejected outright: gap
frequency then scales with trigger frequency rather than simulated time, and a
one-point-per-call configuration degenerates to all-or-nothing per cycle.
Chaos-testing tools that use per-request draws document exactly this coupling.

### Stateful burst-loss model (Gilbert-Elliott)

A two-state good/bad Markov chain produces realistic burst losses, but the
decision at each point depends on hidden state carried from the previous
point — sequential by construction, with the same random-access and
persistence problems as the renewal process, plus a distribution concept the
plugin does not otherwise expose.

## Alternative Configuration Shapes Considered

### Center Plus Jitter

```text
gap_duration_seconds=20
gap_duration_jitter_seconds=10
gap_interval_seconds=180
gap_interval_jitter_seconds=60
```

This matches the timestamp-jitter vocabulary, but the interval arguments are
misleading under a jittered-grid schedule: `gap_interval_jitter_seconds` would
bound each gap's offset from its grid position, while the spacing between
consecutive starts varies by twice that amount (180±60 config actually yields
60–300s spacing). The non-overlap validation rule is also harder to state
(`interval - 2*jitter >= duration + duration_jitter`). Min/max bounds are
directly checkable guarantees, and their validation rule
(`min_interval >= max_duration`) is self-explanatory.

### Probability Per Second

```text
gap_start_probability_per_second=0.005
gap_duration_seconds=20
gap_duration_jitter_seconds=10
```

This maps cleanly to a stochastic process, but users tend to think in "roughly
every few minutes" rather than per-second probabilities.

### Average and Distribution

```text
gap_avg_duration_seconds=20
gap_avg_interval_seconds=180
gap_distribution=exponential
```

This can produce realistic outage timing, with many short intervals and a few
long quiet periods. It is harder to explain, validate, and test, and it adds a
distribution concept that the plugin does not otherwise expose.

### Fixed Plus Random Factor

```text
gap_duration_seconds=20
gap_duration_random_factor=0.5
gap_interval_seconds=180
gap_interval_random_factor=0.25
```

This is compact, but less obvious than duration and jitter expressed directly in
seconds.

## Chosen Design

Use a jittered-grid schedule over absolute time — one gap per `pitch`-wide
stratum, with start offset and duration derived from a `blake2b` hash of
`(seed, gap_index)` — configured with explicit min/max bounds:

```text
gap_min_duration_seconds
gap_max_duration_seconds
gap_min_interval_seconds
gap_max_interval_seconds
gap_seed
gap_enabled
```

The bounds are hard guarantees on outage duration and on spacing between
consecutive outage starts. All gap parameters stay optional while default gaps
remain visible, and the schedule is reproducible, stateless, O(1) to query,
and independent of how many points each scheduled call generates.
