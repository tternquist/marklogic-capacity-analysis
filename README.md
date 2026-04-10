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
| Base ML overhead | Thread stacks, code, bookkeeping (fixed) |
| Page-in / page-out rates | OS paging activity |
| Swap-in / swap-out rates | Non-zero = severe memory pressure |

The key insight for capacity planning: **cache, base overhead, and file cache
are fixed after startup**. Only `forest in-memory stands` grows with document
count. The capacity estimate subtracts fixed components from the memory ceiling
to determine how much room forests have to grow.

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
  `in-memory-triple-index-size` (per-forest budgets)

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

The projection model uses **disk size as the primary estimator** — stress testing
confirmed that disk grows linearly and predictably with document count, while
forest memory fluctuates with merge activity and cached stand pages.

1. **Disk-based projection (primary)**: Divides current disk size by document count
   to get bytes/doc, then estimates how many more documents fit in remaining disk space.
   Validated to within 4% accuracy across 500K+ document loads.
2. **Forest memory indicator (secondary)**: Shown for context but not used for the
   headline projection. `memory-forest-size` includes cached on-disk stand pages
   that get compressed during merges, so it does not scale linearly with doc count.
   For memory-based projections, use `--trend` which measures actual growth rates
   over time.
3. Fragment limits (96M per forest) and stand limits (64 per forest) are checked separately
4. Scaling recommendations flag issues: high fragmentation, stand pressure, memory pressure

**Why disk, not memory?** Stress testing loaded 1.48M documents (10x the initial
count) and found that forest memory grew only ~530 MB while disk grew ~2 GB. Merges
during loading actually *reduced* forest memory even as document count increased.
The disk bytes/doc (1,438 observed) matched the scaling test regression (1,378) to
within 4%, while the forest memory bytes/doc was off by 89%.

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

### Configuration Stability Check

Both `--trend` and `--compare` run a configuration drift check before showing
results. Trend projections are only meaningful when the underlying configuration
stays constant — if someone added a host, resized the cache, or added range
indexes between snapshots, the growth baseline shifted and projections become
unreliable.

Checked fields:

| Category | Fields |
|---|---|
| Cluster | MarkLogic version, host count |
| Per-host | System RAM, ML memory limit, cache allocation (list+tree), CPU cores |
| Database | Forest count, all `in-memory-*` settings |
| Indexes | Range element/path/field index counts, enabled boolean index count |

If the configuration is stable, you'll see:

```
Configuration Stability Check
    STABLE — cluster config, cache sizes, index settings,
    and system resources are consistent across all 5 snapshots.
```

If drift is detected:

```
Configuration Stability Check
    DRIFT DETECTED — configuration changed between snapshots.
    Trend projections may be unreliable across these changes.

    ! In-memory list size (MB)
        256 -> 512  (snapshot #5, 2026-04-11 08:00:00)
    ! Element range index count
        3 -> 5  (snapshot #5, 2026-04-11 08:00:00)
    ! host[ml-1.example.com].cache_alloc_mb
        5121 -> 8192  (snapshot #5, 2026-04-11 08:00:00)

    Tip: For accurate trends, compare snapshots with the same
    configuration. Use --compare N to diff specific snapshots.
```

### Trend Analysis

With 2+ snapshots, `--trend` shows:

- **Configuration stability check** (see above)
- **Growth over time**: Documents, forest disk, forest memory, RSS, and fragments
  with daily rates
- **Runway projections**: Days until memory ceiling, disk full, and fragment limit
  based on observed growth rates (not point-in-time estimates)
- **Binding constraint**: Which resource will be exhausted first

```
Growth Over Time
    Documents:      100,000 -> 250,000  +150,000  (5,000/day)
    Forest disk:    500 MB -> 850 MB    +350 MB   (11.67 MB/day)
    Forest memory:  200 MB -> 340 MB    +140 MB   (4.67 MB/day)

Runway Projections (based on observed growth rate)
    Forest memory headroom:    5.73 GB
    Forest growth rate:        4.67 MB/day
    Days until memory ceiling: 1,257 days
    Binding constraint:        DISK (~120 days at current rate)
```

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
(`in-memory-limit=128MB`, ~2KB average doc), each flush requires ~65,000
documents. The default of 500,000 documents produces 7-8 flushes and an
R² above 0.95.

| Batch Size | Batches | Total Docs | Expected Flushes | Quality |
|---|---|---|---|---|
| 10,000 | 5 | 50,000 | 0-1 | Insufficient |
| 50,000 | 5 | 250,000 | 3-4 | Moderate |
| 50,000 | 10 | 500,000 | 7-8 | Good |
| 100,000 | 10 | 1,000,000 | 15+ | Excellent |

## MarkLogic Memory Model

Understanding MarkLogic's memory architecture is key to capacity planning:

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
        |-- Base ML overhead (FIXED)
        |     |-- Thread stacks, code, internal structures
        |
        |-- File cache / mmap (OS-managed, bounded)
        |
        |-- Forest in-memory stands (GROWS with data)
              |-- In-memory write stand (active ingestion buffer)
              |-- Cached pages of on-disk stands
```

**Fixed components** (cache + base + file cache) are set by configuration and
do not grow with document count. **Forest memory** grows with data but not
linearly — merges compress stand data, and cached pages fluctuate with query
activity. This is why MLCA uses disk-based projections for point-in-time
estimates and `--trend` (actual growth rate) for memory projections.

**Important:** Memory is the binding constraint in the vast majority of
MarkLogic deployments — disk is easy to add, memory is not. While disk-based
projection is the most reliable *point-in-time* metric, the `--trend` analysis
using actual observed `memory-forest-size` growth over time is the best way
to project when memory-based scaling will be needed. Take snapshots regularly
and use `--trend` for memory runway planning.

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

### Option 3: Cron + Textfile Collector (no service needed)

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

## File Structure

```
ml-capacity/
  ml_capacity.py              # CLI + service mode (--serve)
  ml_capacity_test.py         # Scaling validation test
  ml_capacity_stress.py       # One-time stress test
  Dockerfile                     # MLCA service container
  docker-compose.mlca-only.yml   # MLCA only (existing Prometheus + Grafana)
  docker-compose.yml             # MLCA + Prometheus + Grafana (existing ML)
  docker-compose.monitoring.yml  # Full stack including MarkLogic
  monitoring/
    prometheus.yml            # Prometheus scrape config
    grafana/
      dashboards/
        mlca.json             # Pre-built Grafana dashboard
      provisioning/
        dashboards.yml        # Auto-provision dashboard
        datasources.yml       # Prometheus data source
  test/
    docker-compose.yml        # Test ML instance (4GB, for harness)
    test_harness.py           # Memory convergence + index impact tests
  .ml-capacity/               # Snapshot storage (gitignored)
  .gitignore
  README.md
```
