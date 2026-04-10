# MLCA - MarkLogic Capacity Analysis

A zero-dependency Python CLI that connects to a MarkLogic cluster and provides
detailed capacity analysis, scaling projections, and trend tracking over time.

Helps administrators answer:

- How much memory, disk, and fragment headroom does the cluster have?
- Which memory components are fixed (cache, base overhead) vs growing (forest data)?
- Which indexes are consuming the most memory?
- How many more documents can be loaded before hitting resource limits?
- At the current growth rate, when will the cluster need to scale?
- What changed between two points in time?

## Requirements

- Python 3.8+
- MarkLogic 11+ (index memory usage requires ML 11; other features work on ML 10)
- `ML_ALLOW_EVAL=true` for full analysis (forest counts, host memory breakdown, index memory)
- Network access to the MarkLogic Management API (default port 8002)

No external Python packages are needed.

## Quick Start

```bash
# Full capacity report (prompts for password)
python3 ml_capacity.py --host ml.example.com --user admin --auth-type basic

# Analyze a specific database
python3 ml_capacity.py --host ml.example.com --user admin --password secret \
    --database MyContent --auth-type basic

# Save a snapshot without printing the report
python3 ml_capacity.py --host ml.example.com --user admin --password secret \
    --auth-type basic --snapshot-only

# View growth trends from saved snapshots
python3 ml_capacity.py --host ml.example.com --user admin --password secret \
    --auth-type basic --trend

# Compare current state to an earlier snapshot
python3 ml_capacity.py --host ml.example.com --user admin --password secret \
    --auth-type basic --compare 0
```

---

## Capacity Model Methodology

This section documents the formulas, assumptions, and validation behind
MLCA's capacity projections. It is intended for product engineers who
need to audit, validate, or extend the model.

### MarkLogic Memory Architecture

Understanding how MarkLogic allocates memory is foundational to
the capacity model. The process RSS (resident set size) is decomposed
into components with fundamentally different scaling behavior:

```
System RAM
  |
  |-- OS + other processes
  |
  |-- MarkLogic process (RSS)
        |
        |-- Cache allocation (FIXED after startup)
        |     |-- List cache          (group setting: list-cache-size)
        |     |-- Compressed tree cache (group setting: compressed-tree-cache-size)
        |
        |-- File cache / mmap (OS-managed, bounded)
        |
        |-- Forest in-memory stands (GROWS with data + ingestion)
        |     |-- In-memory write stand (active ingestion buffer)
        |     |-- Cached pages of on-disk stands
        |
        |-- Base overhead (threads, code, bookkeeping)
```

**Fixed components** — cache allocation, file cache, and base overhead —
are set by configuration and do not grow with document count. Only
**forest memory** grows with data, and it does so non-linearly because
merges compress stand data and cached pages fluctuate with query
activity. This asymmetry is why MLCA uses different strategies for
point-in-time estimates (disk-based) vs runway projections (trend-based).

### Data Sources

All metrics are collected from MarkLogic's Management API and built-in
status functions. No operating system calls or external agents are needed.

| Source | Metrics | Used For |
|--------|---------|----------|
| `xdmp:host-status()` | RSS, swap, cache alloc, forest memory, file cache, host-size, system free/total, page rates | Memory ceiling, headroom, component breakdown |
| `xdmp:forest-status()` | Per-forest disk-size, memory-size, stand count | Disk projection, stand limits |
| `xdmp:forest-counts()` | Document count, active/deleted/nascent fragment counts | Fragment limits, fragmentation ratio |
| `/manage/v2/databases/{db}/properties` | `in-memory-limit`, `in-memory-list-size`, `in-memory-tree-size`, `in-memory-range-index-size`, `in-memory-reverse-index-size`, `in-memory-triple-index-size`, range indexes | In-memory stand budget, index config |
| `/manage/v2/databases/{db}?view=status` | Storage sizes, cache hit ratios, merge/reindex/backup counts | Database health, disk remaining |
| `xdmp.databaseDescribeIndexes()` (ML 11+) | Per-index definitions | Index memory attribution |
| `xdmp.forestStatus(forests, 'memoryDetail')` (ML 11+) | Per-stand, per-index byte counts | Index memory/disk breakdown |

### Ceiling Calculation

The memory ceiling defines how much RAM the MarkLogic process should
use before the cluster is under memory pressure.

```
safe_cap = min(ml_limit, system_ram * 0.80)
```

| Variable | Source | Fallback |
|----------|--------|----------|
| `ml_limit` | `memory-size` from `xdmp:host-status()` — the configured group memory limit | None |
| `system_ram` | `memory-system-total` from `xdmp:host-status()` | If 0 (containers), use `ml_limit` directly |

The **80% guardrail** reserves 20% of system RAM for the operating system,
page caches, and other processes. This is a standard recommendation from
MarkLogic documentation and is conservative — production clusters often
operate at 70-75% without issue.

**Container fallback**: Containerized MarkLogic (Docker, Kubernetes) often
reports `memory-system-total = 0` or `memory-system-free = 0` because
cgroup memory accounting doesn't surface these metrics the same way as
bare-metal. When system RAM is unavailable, the configured ML limit
(`memory-size`) is used as the ceiling instead.

### Headroom Analysis

Headroom is computed at two levels — the at-rest state and the during-load state:

```
headroom_at_rest    = safe_cap - rss
headroom_during_load = headroom_at_rest - in_mem_total_all
```

Where `in_mem_total_all` is the in-memory stand reserve (see below).
A snapshot taken at rest will not show in-memory stand memory as consumed,
but it **will** be occupied the moment a bulk load begins. If
`headroom_during_load < 0`, a large ingestion job will push RSS over
the ceiling before the data even reaches disk.

### In-Memory Stand Reserve

When MarkLogic ingests documents, they first land in an **in-memory stand** —
a write-optimized but uncompressed structure held in RAM. When the stand
reaches the flush threshold (`in-memory-limit` KB), it is written to disk
as a compressed on-disk stand and the in-memory stand is released.

The peak RAM consumed by one in-memory stand is the sum of five
configurable database properties:

```
in_mem_per_forest = in-memory-list-size
                  + in-memory-tree-size
                  + in-memory-range-index-size
                  + in-memory-reverse-index-size
                  + in-memory-triple-index-size

in_mem_total_all  = in_mem_per_forest * num_forests
```

| Setting | ML Default | Description |
|---------|-----------|-------------|
| `in-memory-list-size` | 64 MB | Word and element list data |
| `in-memory-tree-size` | 16 MB | Compressed tree data |
| `in-memory-range-index-size` | 2 MB | Range index entries |
| `in-memory-reverse-index-size` | 2 MB | Reverse index entries |
| `in-memory-triple-index-size` | 16 MB | Triple index (semantics) |
| **Total per forest** | **100 MB** | |

These defaults were confirmed against the MarkLogic Management API and
**validated experimentally**: on a 4 GB Docker instance with default
settings (sum = 100 MB), the observed peak in-memory stand size was
102 MB — within 2% of the predicted value. See
[Validation: In-Memory Stand Overhead](#validation-in-memory-stand-overhead).

**Important for tuned clusters**: Administrators frequently adjust these
settings for throughput. A cluster with `in-memory-list-size = 1024 MB`
will have a 1 GB+ ingestion reserve per forest — the model reads the
actual configured values, not the defaults.

**In-memory vs on-disk stand ratio**: The in-memory stand is approximately
**2.9x larger** than the equivalent on-disk stand for the same documents
(validated: 102 MB in-memory flushed to 35 MB on-disk). This ratio
reflects the compression and deduplication that occurs during the flush.

### Disk-Based Document Projection (Primary)

Disk size grows linearly and predictably with document count. This makes
it the most reliable single-snapshot capacity estimator. Forest memory
fluctuates with merge activity, cached stand pages, and query patterns,
making it unreliable for point-in-time projections.

```
disk_bytes_per_doc = total_forest_disk / total_docs
docs_remaining     = remaining_disk / disk_bytes_per_doc
```

**Why disk, not memory?** Stress testing loaded 1.48M documents and found
that forest memory grew only ~530 MB while disk grew ~2 GB. Merges during
loading actually *reduced* forest memory even as document count increased.
The disk `bytes/doc` matched the stress test regression to within 4%
(R² = 0.95), while the forest memory `bytes/doc` was off by 89%.

#### Fragmentation Adjustment

When fragmentation (deleted fragments / total fragments) exceeds 25%,
the raw `disk_bytes_per_doc` is inflated because deleted fragments still
occupy space in stands. MLCA applies a conservative correction:

```
avg_bytes_per_frag = total_disk / total_fragments
estimated_waste    = deleted_fragments * avg_bytes_per_frag * 0.50
clean_disk         = max(total_disk - estimated_waste, total_disk * 0.50)
clean_bytes_per_doc = clean_disk / total_docs
```

**Assumptions**:

1. **50% waste factor**: Deleted fragments don't occupy space proportional
   to their count — they share stands with active fragments, and document
   sizes vary. We assume deleted fragments occupy at most half the average
   per-fragment size. Validated: the raw active/total ratio overpredicts
   reclamation by ~40% in testing; the 50% factor produces 88-94% accuracy
   vs post-merge actuals.
2. **50% floor**: Even with extreme fragmentation, we assume at least half
   the current disk will persist after a merge. This prevents unrealistically
   optimistic projections.
3. **Thresholds**: >= 25% deleted triggers a warning and adjusts projections;
   >= 50% marks projections as UNRELIABLE until a merge completes.

### Memory Runway Projection (Trend-Based)

For memory-based scaling projections, point-in-time snapshots are
unreliable because forest memory fluctuates with merges. The `--trend`
mode uses multiple snapshots over time to fit a growth curve and
project when the memory ceiling will be reached.

#### Regression Model

MLCA uses **documents as the x-axis** (not time) for regression, because
document count is stable, monotonic, and independent of loading rate:

```
total_memory = slope * doc_count + intercept    (linear regression)
```

Where `total_memory` is `forest_mem + base_overhead + file_cache`
(the variable components). The slope gives **marginal bytes per document**
and the intercept captures fixed overhead.

The **R² statistic** indicates fit quality:

| R² | Interpretation |
|----|---------------|
| > 0.90 | Strong linear relationship; projections are reliable |
| 0.70 – 0.90 | Moderate; projections are directional |
| < 0.70 | Poor; insufficient data or non-linear growth |

#### Merge Spike Filtering

Forest memory can spike dramatically during merges (old + new stands
coexist temporarily). These spikes would distort the regression, so
MLCA filters them:

```
spike_threshold = median(forest_mb) * 1.6
```

Snapshots where `forest_mb > spike_threshold` are excluded from the
regression. If fewer than 3 non-spike snapshots remain, filtering is
disabled and all points are used.

#### Snapshot Deduplication

Multiple snapshots at the same document count (e.g., taken between
ingestion phases) are deduplicated: for each run of identical doc
counts, only the first and last snapshot are kept. This captures both
the inflection point (merge just completed) and the settled state
(post-merge).

#### Runway Calculation

```
daily_forest_growth = forest_delta / days_elapsed
days_until_ceiling  = forest_headroom / daily_forest_growth

daily_disk_growth   = disk_delta / days_elapsed
days_until_disk     = remaining_disk / daily_disk_growth

daily_frag_growth   = fragment_delta / days_elapsed
days_until_frag     = fragments_remaining / daily_frag_growth

binding_constraint  = min(days_until_ceiling, days_until_disk, days_until_frag)
```

### Document Capacity Projection (Forest Headroom)

The forest headroom calculation determines how much RAM is available for
additional data:

```
fixed_mem              = cache_alloc + base_overhead + file_cache
forest_headroom        = safe_cap - fixed_mem - forest_mem
forest_headroom_loading = forest_headroom - in_mem_total_all
```

During ingestion, one in-memory stand per forest is held in RAM, so
`forest_headroom_loading` is the operative headroom during bulk loads.

### Configuration Drift Detection

Trend projections are only meaningful when the underlying configuration
is stable. If cache sizes, index counts, or host resources change between
snapshots, the growth baseline shifts and projections become unreliable.

MLCA compares a configuration fingerprint across all snapshots used for
trending. Checked fields:

| Category | Fields |
|----------|--------|
| Cluster | MarkLogic version, host count |
| Per-host | System RAM, ML memory limit, cache allocation, CPU cores |
| Database | Forest count, all `in-memory-*` settings |
| Indexes | Range element/path/field index counts, enabled boolean index count |

**Fuzzy tolerance**: Memory fields (`system_ram_mb`, `ml_limit_mb`,
`cache_alloc_mb`) use `max(2 MB, 0.5% of value)` tolerance to handle
API rounding noise. All other fields (counts, strings, versions) use
exact equality.

**None/0 handling**: Values of `None` or `0` are treated as "unknown"
and do not trigger drift. This avoids false alarms in containers where
OS metrics start as 0 but populate later. The trade-off is that removal
of ALL range indexes (count → 0) will not be flagged, but this is
extremely rare compared to container false alarms.

---

## Known Limitations and Assumptions

### host-size is a Disk Metric, Not Memory

`xdmp:host-status()/*:host-size` reports the **total on-disk size of all
forests on the host** — it is a storage metric despite appearing alongside
memory metrics in the host status output.

**Validated empirically**: When the Documents forest disk was cleared to 0,
host-size dropped to 4 MB (= App-Services 1 + Meters 1 + Security 2 MB
— the system forest disks). During a 12-phase ceiling test, host-size
tracked forest disk exactly, with a constant 4 MB offset.

The capacity model includes `base_overhead` (host-size) in `fixed_mem`.
In constrained containers this works coincidentally because the OS
page-caches all forest files (disk ≈ RAM pressure). On production
clusters with terabytes of forest data, this would produce incorrect
headroom calculations. This is a known open issue.

### system_free_mb = 0 in Containers

Containerized MarkLogic returns 0 for `memory-system-free` because cgroup
memory accounting does not surface free memory the same way as bare-metal.
Zero means "metric unavailable," not "zero free memory." All safety
guards treat `free_mb == 0` as "unknown" and skip the check rather than
triggering a false alarm.

### Forest Memory is Non-Linear

`memory-forest-size` includes both in-memory stand data and cached pages
of on-disk stands. Merges compress stands and release cached pages,
causing forest memory to *decrease* even as document count increases.
This is why MLCA uses disk for point-in-time projections and multi-snapshot
trends for memory runway estimation.

### Per-Forest Overhead is Zero (Empty Forests)

Adding empty forests to a database incurs no measurable overhead in disk,
RSS, or forest memory. MarkLogic allocates resources only when a forest
contains data. The capacity model does not include a per-forest fixed
cost factor.

**Validated**: Adding 2 empty forests to the Documents database on a
4 GB Docker instance produced 0.0 MB delta across all metrics.

### Fragment Limit is Per-Forest

Each forest supports up to 96,000,000 fragments. The total fragment
capacity scales linearly with forest count. MLCA tracks fragment
utilization and projects when the limit will be reached based on the
observed doc-to-fragment ratio.

### Stand Limit is 64 Per Forest

Each forest can hold a maximum of 64 on-disk stands. If stand count
reaches 64, further ingestion fails until a merge completes. MLCA
tracks stand count per forest and warns when it exceeds 48.

---

## Validation Experiments

The capacity model has been validated through a series of controlled
experiments on a memory-constrained Docker instance (4 GB limit,
MarkLogic latest, default settings).

### Test Environment

```yaml
# test/docker-compose.yml
marklogic:
  image: progressofficial/marklogic-db:latest
  mem_limit: 4g
  ports:
    - "8100:8000"   # Query Console
    - "8101:8001"   # Admin Interface
    - "8102:8002"   # Management API
```

The 4 GB memory limit makes the memory ceiling (80% = 3.2 GB) reachable
with a modest number of documents (~500K-1M), allowing ceiling tests
to run in minutes rather than hours.

### Validation: Disk Scaling Linearity

**Script**: `ml_capacity_test.py`

Loads 500K+ documents in batches of 50K, takes metrics after each batch,
and runs linear regression on `disk_size ~ doc_count`.

**Results**:
- Disk grows linearly with R² > 0.95
- Observed bytes/doc (1,438) matched the single-snapshot estimate (1,378)
  to within 4%
- Forest memory bytes/doc was off by 89% (unreliable for point-in-time)

**Conclusion**: Disk-based projection is the correct primary estimator
for single-snapshot capacity reports.

### Validation: Memory Trend Convergence

**Script**: `test/test_harness.py`

Loads documents in 5+ phases on a clean 4 GB instance, takes snapshots
after each phase, and runs trend analysis to check whether the memory
runway projection converges.

**Results**:
- Projections converge within 7.5% of actual after 5+ phases
- Configuration drift checks remain STABLE throughout
- Trend-based `--trend` is the correct approach for memory runway planning

### Validation: Ceiling Test (Telemetry Schema)

**Script**: `test/test_ceiling.py`

Uses a different document schema (IoT telemetry events) with four
pre-installed range indexes (`/value` double, `/severity` int,
`/metric` string, `/event_id` string) to validate the model against
a non-trivial index configuration.

Documents are loaded via **Flux** through the flux-runner HTTP API
(no docker cp — see `MCP_FRICTION_LOG.md` for details on the
HTTP-serve pattern).

**Results** (12 phases of 100K docs, pushed to 91.9% of 3.2 GB ceiling):
- `forest_bytes_per_doc` converged to ~130 bytes/doc (2% deviation) after phase 9
  — this captures only stand memory, not the full per-doc cost
- `disk_bytes_per_doc` = ~1,476 bytes/doc — the meaningful capacity planning number
  that includes range index overhead in the on-disk stands
- The harness stopped cleanly at the ceiling; no OOM or swap

### Validation: Per-Forest Overhead

**Script**: `test/test_forest_overhead.py`

Adds 2 additional forests to the Documents database, measures overhead
when empty, loads 100K documents, and measures per-doc cost across
3 forests.

**Results**:
- Empty forest overhead = 0 for all metrics (disk, RSS, forest-mem)
- 100K docs distributed evenly via rebalancer: 33,333/forest
- Per-doc cost identical regardless of forest count
- Range index disk overhead isolated: ~270 bytes/doc
  (1,476 with 4 indexes − 1,206 without)

**Conclusion**: Forest count is not a variable in the capacity model.
The model treats forest memory/disk as a single pool.

### Validation: In-Memory Stand Overhead

**Script**: `test/test_inmemory_stands.py`

Runs a Flux bulk load in a background thread while polling MarkLogic
memory metrics every 2 seconds. Captures the peak memory pressure
during ingestion vs the settled post-load state.

**Results** (200K docs, 1 forest, default in-memory settings):

| Metric | Baseline | Peak (during load) | Settled (post-load) |
|--------|----------|-------------------|-------------------|
| forest-mem (all) | 554 MB | 675 MB (+16.6%) | 579 MB |
| process-rss | 1,085 MB | 1,637 MB (+2.1%) | 1,604 MB |

- In-memory stand peak: ~102 MB (predicted from settings sum: 100 MB — 2% error)
- In-memory:on-disk ratio: **2.9x** (102 MB flushed to 35 MB per flush cycle)
- RSS spike during load: only +2.1% — RSS is a stable metric
- Settle time after load completes: ~20 seconds
- Disk temporarily 2x during merge (old + new stands coexist)

The ~100 MB overhead is a **fixed constant per forest** (determined by
the `in-memory-*` settings), not proportional to data size. For large
loads the burst factor converges toward 1.0x + one in-memory stand.

---

## Report Sections

A full report contains seven sections:

### 1. Cluster Overview

Cluster name, MarkLogic version, and counts of hosts, databases, forests, and
app servers.

### 2. Host Memory

Per-host breakdown of system and MarkLogic memory using `xdmp:host-status()`:

| Component | Description |
|---|---|
| System total / free / used | OS-level RAM |
| ML Virtual (VSZ) | Full process address space |
| ML Resident (RSS) | Physical pages currently in use |
| RSS peak (HWM) | High water mark since last restart |
| Anonymous/heap | Heap allocations (subset of RSS) |
| Swap | Pages swapped out (non-zero = pressure) |
| Cache alloc (list+tree) | Pre-allocated list cache + compressed tree cache (fixed) |
| Forest in-memory stands | In-memory stand data across all forests (grows with data) |
| File cache (mmap) | OS file cache pages held by ML |
| Base ML overhead | Thread stacks, code, bookkeeping |
| Page-in / page-out rates | OS paging activity |
| Swap-in / swap-out rates | Non-zero = severe memory pressure |

### 3. Database Statistics

Storage sizes, disk utilization with runway estimate, cache hit ratios, and
active merge/reindex/backup counts.

### 4. Forest Health

Per-forest metrics via `xdmp:forest-counts()`:

- Document and fragment counts
- Fragmentation percentage (deleted / total fragments)
- Stand count vs 64-stand limit with utilization bar
- Disk and in-memory size
- Fragment capacity vs 96M-per-forest limit

### 5. Index Configuration

Lists all enabled/disabled boolean indexes, range indexes (element, path, field),
and the in-memory settings that control stand allocation:

- `in-memory-limit` (flush threshold in KB)
- `in-memory-list-size`, `in-memory-tree-size`, `in-memory-range-index-size`,
  `in-memory-reverse-index-size`, `in-memory-triple-index-size` (per-forest budgets)

### 6. Index Memory Usage

Per-index memory and disk usage using MarkLogic 11+ APIs:

- **Stand memory breakdown**: Shows where forest memory is consumed across all
  stands (range indexes, timestamps, triple index, list/tree, keys, etc.) with
  percentages. This tells you which component types dominate memory.
- **Per-index detail**: Memory and on-disk bytes for each URI lexicon, collection
  lexicon, range element index, path range index, etc. Useful for identifying
  which specific indexes are most expensive.

Uses `xdmp.databaseDescribeIndexes()` for index definitions and
`xdmp.forestStatus(forests, 'memoryDetail')` for per-stand, per-index byte counts.

### 7. Capacity Estimate

Combines all the model components into a single report:

1. **Memory breakdown**: Fixed components (cache + base + file) vs growing (forest)
2. **Headroom analysis**: At-rest headroom, in-memory stand reserve, headroom during bulk load
3. **Disk projection** (primary): bytes/doc, docs remaining, fragmentation adjustment
4. **Memory runway** (secondary): forest headroom, per-doc snapshot cost, pointer to `--trend`
5. **Fragment and stand limits**: Per-forest utilization with warnings
6. **Scaling recommendations**: Actionable warnings for detected issues

---

## Snapshots and Trending

Every run automatically saves a snapshot to `.ml-capacity/` (next to the script).
Snapshots capture all metrics in a single JSON file for later analysis.

### CLI Flags

| Flag | Description |
|---|---|
| `--snapshot-only` | Save a snapshot without printing the full report |
| `--no-snapshot` | Run the report without saving a snapshot |
| `--snapshots` | List all saved snapshots (no server connection needed) |
| `--trend` | Show growth curves and runway projections from saved snapshots |
| `--compare N` | Diff the most recent snapshot vs snapshot #N |
| `--index-impact` | Show index memory impact between 2 most recent snapshots |
| `--project-docs N` | Project index costs at N documents (use with `--index-impact`) |
| `--import-snapshot FILE...` | Import snapshot JSON files from disconnected environments |

### Disconnected Environments

For MarkLogic clusters that are not network-reachable from the machine running
MLCA (air-gapped, VPN-restricted, different data centers), you can collect
snapshots directly on the cluster and import them later.

**Step 1: Collect a snapshot on the remote cluster**

Copy `scripts/collect-snapshot.sjs` to the target environment and run it via
Query Console or curl:

```bash
# Via Query Console:
#   1. Open Query Console on the target cluster
#   2. Paste the contents of scripts/collect-snapshot.sjs
#   3. Edit the DATABASE variable at the top of the script
#   4. Run it and copy the JSON output to a file

# Via curl:
curl -s --anyauth -u admin:password \
  -X POST http://remote-host:8000/v1/eval \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "javascript=$(cat scripts/collect-snapshot.sjs)" \
  | python3 -c "
import sys, json
for part in sys.stdin.read().split('--'):
    if 'application/json' in part:
        body = part.split('\r\n\r\n', 1)[1].strip()
        if body:
            print(json.dumps(json.loads(body), indent=2))
" > snapshot_remote_$(date +%Y%m%d).json
```

The script collects the same data as a live MLCA run: host memory, forest
counts, database properties, index configuration, and per-index memory detail
(ML 11+).

**Required privileges:** `xdmp:host-status`, `xdmp:forest-status`,
`xdmp:forest-counts`, `xdmp:hosts`, `xdmp:database-forests`. For index memory
detail: `xdmp:database-describe-indexes` (ML 11+).

**Step 2: Import the snapshot into MLCA**

```bash
# Import one or more snapshot files
python3 ml_capacity.py --import-snapshot snapshot_remote_*.json

# Then use all the usual analysis commands
python3 ml_capacity.py --database Documents --trend
python3 ml_capacity.py --database Documents --compare 0
```

The import validates the JSON structure and saves each file into `.ml-capacity/`
using the standard naming convention. No server connection is needed.

### Configuration Stability Check

Both `--trend` and `--compare` run a configuration drift check before showing
results. See [Configuration Drift Detection](#configuration-drift-detection)
for details on what is checked and the tolerance model.

### Trend Analysis

With 2+ snapshots, `--trend` shows:

- **Configuration stability check**
- **Growth over time**: Documents, forest disk, forest memory, RSS, and fragments
  with daily rates
- **Runway projections**: Days until memory ceiling, disk full, and fragment limit
  based on observed growth rates
- **Binding constraint**: Which resource will be exhausted first

### Comparison

`--compare 0` diffs the current state vs snapshot #0:

- **Configuration stability check** between the two snapshots
- Metric deltas with absolute and percentage changes
- Marginal cost per document (disk bytes/doc, forest memory bytes/doc) computed
  from actual growth between the two snapshots
- Index configuration changes (added/removed indexes between snapshots)

### Scheduling Snapshots

For trend analysis to be useful, take snapshots regularly:

```bash
# Cron: daily snapshot at 2am
0 2 * * * python3 /path/to/ml_capacity.py \
    --host ml.example.com --user admin --password secret \
    --auth-type basic --snapshot-only
```

Then check trends periodically:

```bash
python3 ml_capacity.py --host ml.example.com --user admin --password secret \
    --auth-type basic --trend
```

---

## Service Mode and Prometheus Metrics

MLCA can run as a persistent service that collects metrics on a schedule and
exposes them for Prometheus scraping, while also serving a lightweight web UI.

All existing CLI flags continue to work — `--serve` is additive.

### Starting the Service

```bash
# Service mode — collects every 15 minutes, serves on port 9090
python3 ml_capacity.py --host ml.example.com --user admin --password secret \
    --serve --interval 15m --serve-port 9090

# One-shot Prometheus output (no service, useful for cron/textfile collector)
python3 ml_capacity.py --host ml.example.com --user admin --password secret \
    --format prometheus

# One-shot JSON output
python3 ml_capacity.py --host ml.example.com --user admin --password secret \
    --format json
```

### Service Endpoints

| Endpoint | Description |
|---|---|
| `http://localhost:9090/` | Web UI — memory hero metrics, gauges, host breakdown, index table |
| `http://localhost:9090/metrics` | Prometheus scrape endpoint (30+ metrics) |
| `http://localhost:9090/api/snapshot` | Latest snapshot as JSON |
| `http://localhost:9090/api/snapshots` | All snapshot summaries as JSON |
| `http://localhost:9090/api/trend` | Time-series data for charting |
| `http://localhost:9090/health` | Health check (`{"status":"ok"}`) |

### Prometheus Metrics

All metrics use the `mlca_` prefix with labels for `database` and `host`:

```
# Memory (most important for capacity planning)
mlca_memory_headroom_mb{database="Documents"}        5730
mlca_memory_ceiling_mb{database="Documents"}         12493
mlca_memory_utilization_ratio{database="Documents"}  0.54
mlca_host_forest_mb{host="ml-1.example.com"}         518
mlca_host_rss_mb{host="ml-1.example.com"}            1285
mlca_host_swap_mb{host="ml-1.example.com"}           0
mlca_host_cache_mb{host="ml-1.example.com"}          5120

# Documents and fragments
mlca_documents_total{database="Documents"}           162598
mlca_fragmentation_ratio{database="Documents"}       0.0025

# Disk
mlca_forest_disk_mb{database="Documents"}            334
mlca_disk_remaining_mb{database="Documents"}         142040

# Per-index memory (ML 11+)
mlca_index_memory_bytes{database="Documents",index="uriLexicon"}  4388288

# Stand memory components
mlca_stand_range_indexes_bytes{database="Documents"} 31798620
```

### Optional OTLP Push

Push metrics to any OpenTelemetry Collector without additional dependencies:

```bash
python3 ml_capacity.py --host ml.example.com --user admin --password secret \
    --serve --otlp-endpoint http://otel-collector:4318
```

---

## Grafana Integration

MLCA ships with a pre-built Grafana dashboard and the configuration needed
to get from zero to a working monitoring stack in minutes.

### Docker Compose Options

Three compose files for different environments:

| File | What it starts | When to use |
|---|---|---|
| `docker-compose.mlca-only.yml` | MLCA only | You already have Prometheus + Grafana |
| `docker-compose.yml` | MLCA + Prometheus + Grafana | You have MarkLogic but no monitoring stack |
| `docker-compose.monitoring.yml` | Everything (ML + MLCA + Prometheus + Grafana) | Self-contained test/demo environment |

All compose files accept the same environment variables:

| Variable | Default | Description |
|---|---|---|
| `MLCA_HOST` | `localhost` | MarkLogic Management API host |
| `MLCA_PORT` | `8002` | Management API port |
| `MLCA_USER` | `admin` | MarkLogic username |
| `MLCA_PASSWORD` | `admin` | MarkLogic password |
| `MLCA_AUTH_TYPE` | `digest` | `digest` or `basic` |
| `MLCA_DATABASE` | `Documents` | Database to monitor |
| `MLCA_INTERVAL` | `15m` | Collection interval (`5m`, `15m`, `1h`) |
| `GRAFANA_PASSWORD` | `admin` | Grafana admin password |

### Option 1: MLCA Only (existing Prometheus + Grafana)

If you already have a Prometheus/Grafana stack, just start the MLCA service
and point your existing Prometheus at it:

```bash
MLCA_HOST=ml.example.com MLCA_PASSWORD=secret \
  docker compose -f docker-compose.mlca-only.yml up -d
```

Then add the scrape target to your `prometheus.yml` (see Step 2 below) and
import the dashboard into Grafana (see Step 3).

### Option 2: MLCA + Prometheus + Grafana (existing MarkLogic)

The full monitoring stack connecting to an existing MarkLogic cluster.
Grafana dashboard is auto-provisioned.

```bash
MLCA_HOST=ml.example.com MLCA_PASSWORD=secret docker compose up -d

# If MarkLogic is running on the same Docker host:
MLCA_HOST=host.docker.internal docker compose up -d
```

Access:
- **Grafana**: http://localhost:3000 (login: admin / admin)
- **MLCA Web UI**: http://localhost:9090
- **Prometheus**: http://localhost:9091

The Grafana dashboard is auto-provisioned — no manual import needed. Open
Grafana, go to Dashboards, and find "MLCA — MarkLogic Capacity Analysis"
under the MarkLogic folder.

To stop: `docker compose down` (add `-v` to also delete stored data).

### Option 3: Full Stack Including MarkLogic (test/demo)

Self-contained environment for testing or demos:

```bash
docker compose -f docker-compose.monitoring.yml up -d
```

Starts MarkLogic (ports 8000-8002), MLCA, Prometheus, and Grafana.

### Manual Setup: Add MLCA to an Existing Prometheus/Grafana

If you don't use Docker or want to run MLCA directly:

**Step 1: Start the MLCA service**

```bash
# Run directly
python3 ml_capacity.py --host ml.example.com --user admin --password secret \
    --auth-type basic --serve --interval 15m --serve-port 9090

# Or via Docker
docker build -t mlca .
docker run -d --name mlca -p 9090:9090 \
    -e MLCA_HOST=ml.example.com \
    -e MLCA_PASSWORD=secret \
    -e MLCA_AUTH_TYPE=basic \
    mlca
```

**Step 2: Add MLCA as a Prometheus scrape target**

Add to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'mlca'
    scrape_interval: 60s
    static_configs:
      - targets: ['mlca-host:9090']  # wherever MLCA is running
```

Reload Prometheus: `curl -X POST http://prometheus:9090/-/reload`

Verify metrics appear: open Prometheus UI, query `mlca_documents_total`.

**Step 3: Import the Grafana dashboard**

1. In Grafana, go to **Dashboards → Import**
2. Upload `monitoring/grafana/dashboards/mlca.json` (or paste its contents)
3. Select your Prometheus data source
4. Click **Import**

### Option 4: Cron + Textfile Collector (no service needed)

If you use Prometheus Node Exporter with the textfile collector, you can
write Prometheus metrics to a file on a schedule — no MLCA service needed:

```bash
# Cron job: collect metrics every 15 minutes
*/15 * * * * python3 /path/to/ml_capacity.py \
    --host ml.example.com --user admin --password secret \
    --auth-type basic --format prometheus --no-snapshot \
    > /var/lib/node_exporter/textfile_collector/mlca.prom
```

The Node Exporter will pick up the file and expose the metrics at its
own `/metrics` endpoint, which Prometheus already scrapes.

### Grafana Dashboard Panels

The pre-built dashboard includes:

| Row | Panels |
|---|---|
| **Memory (hero)** | Memory headroom (stat), ceiling usage (gauge), documents (stat), fragmentation (stat), swap alert (stat) |
| **Time Series** | Forest memory over time with ceiling line, RSS over time with ceiling line |
| **Breakdown** | Memory component stacked bar (cache/base/forest/file), document growth, disk usage |
| **Indexes** | Per-index memory table, stand memory component pie chart |

**Recommended alerts** to configure in Grafana:

| Alert | Condition | Severity |
|---|---|---|
| Memory runway low | `mlca_memory_headroom_mb < 2048` | Warning |
| Memory critical | `mlca_memory_headroom_mb < 512` | Critical |
| Swap detected | `mlca_host_swap_mb > 0` | Critical |
| High fragmentation | `mlca_fragmentation_ratio > 0.25` | Warning |
| RSS near ceiling | `mlca_memory_utilization_ratio > 0.85` | Warning |

---

## Scaling Validation Test

`ml_capacity_test.py` validates the capacity model by loading documents and
measuring how metrics actually scale:

```bash
# Default: 10 batches x 50,000 docs = 500,000 documents
python3 ml_capacity_test.py --host ml.example.com --user admin --password secret \
    --auth-type basic

# Smaller test run
python3 ml_capacity_test.py --host ml.example.com --user admin --password secret \
    --auth-type basic --batches 5 --batch-size 10000

# Keep test documents after the run
python3 ml_capacity_test.py --host ml.example.com --user admin --password secret \
    --auth-type basic --no-cleanup
```

The test:

1. **Generates documents server-side** (SJS eval) with randomized structure
   (small/medium/large, varying fields). Only count + offset are sent as
   parameters, so batch size is unlimited.
2. **Samples metrics after each batch**: disk size, in-memory write stand,
   stand count, host forest memory, RSS.
3. **Runs linear regression** on disk-size vs document count to compute
   bytes/doc on disk and R² to confirm linear scaling.
4. **Validates** that stand flushes occurred, bytes/doc is plausible, and
   disk growth is linear.
5. **Compares** the observed regression slope against `ml_capacity.py`'s
   snapshot-based estimate.
6. **Cleans up** all test documents (unless `--no-cleanup`).

### Sizing Recommendations

To get a reliable regression signal, you need enough documents to trigger
multiple in-memory stand flushes. With default settings
(`in-memory-limit=32768 KB`, ~2KB average doc), each flush requires ~16,000-30,000
documents depending on document structure. The default of 500,000 documents
produces multiple flushes and an R² above 0.95.

| Batch Size | Batches | Total Docs | Expected Flushes | Quality |
|---|---|---|---|---|
| 10,000 | 5 | 50,000 | 0-1 | Insufficient |
| 50,000 | 5 | 250,000 | 3-4 | Moderate |
| 50,000 | 10 | 500,000 | 7-8 | Good |
| 100,000 | 10 | 1,000,000 | 15+ | Excellent |

## Stress Test

`ml_capacity_stress.py` is a one-time test that loads documents toward the
projected memory ceiling to validate capacity projections:

```bash
# Default: load to 75% of projected ceiling in 100K-doc waves
python3 ml_capacity_stress.py --host ml.example.com --user admin --password secret \
    --auth-type basic

# Adjust target percentage or wave size
python3 ml_capacity_stress.py --host ml.example.com --user admin --password secret \
    --auth-type basic --target-pct 50 --wave-size 50000
```

The test includes safety stops for swap detection, memory threshold breach, and
low system free memory. It takes before/after snapshots and reports actual vs
projected per-document costs.

---

## Key Constants

| Constant | Value | Rationale |
|----------|-------|-----------|
| Memory ceiling factor | 80% of system RAM | Standard ML recommendation; reserves 20% for OS |
| Fragment limit per forest | 96,000,000 | MarkLogic architectural limit |
| Stand limit per forest | 64 | MarkLogic architectural limit |
| Fragmentation warning threshold | 25% deleted | Point where projections start inflating |
| Fragmentation critical threshold | 50% deleted | Projections unreliable; merge needed |
| Deleted fragment waste factor | 50% | Conservative; raw ratio overpredicts by 40% |
| Fragmentation floor | 50% of current disk | Prevents over-optimistic merge reclamation estimates |
| Merge spike detection | 1.6x median forest_mb | Filters mid-merge snapshots from regression |
| Short window threshold | < 12 hours | Below this, time-based ETA projections are suppressed |
| Fuzzy MB tolerance | max(2 MB, 0.5%) | Handles API rounding noise in config drift checks |
| Default in-memory budget per forest | 100 MB | 64 + 16 + 2 + 2 + 16 (ML defaults, confirmed via API) |

---

## File Structure

```
ml-capacity/
  ml_capacity.py                 # CLI + service mode (--serve)
  ml_capacity_test.py            # Scaling validation test
  ml_capacity_stress.py          # One-time stress test
  scripts/
    collect-snapshot.sjs         # SJS script for disconnected snapshot collection
  Dockerfile                     # MLCA service container
  docker-compose.mlca-only.yml   # MLCA only (existing Prometheus + Grafana)
  docker-compose.yml             # MLCA + Prometheus + Grafana (existing ML)
  docker-compose.monitoring.yml  # Full stack including MarkLogic
  monitoring/
    prometheus.yml               # Prometheus scrape config
    grafana/
      dashboards/
        mlca.json                # Pre-built Grafana dashboard
      provisioning/
        dashboards.yml           # Auto-provision dashboard
        datasources.yml          # Prometheus data source
  test/
    docker-compose.yml           # Test ML instance (4GB, for validation harnesses)
    test_harness.py              # Memory convergence + index impact tests
    test_ceiling.py              # Telemetry schema stress test with Flux loader
    test_forest_overhead.py      # Per-forest/stand overhead measurement
    test_inmemory_stands.py      # In-memory stand peak monitoring during load
  .ml-capacity/                  # Snapshot storage (gitignored)
  MCP_FRICTION_LOG.md            # Friction log for MCP tool development
  .gitignore
  README.md
```
