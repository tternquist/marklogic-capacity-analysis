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

The projection model:

1. **Forest memory** is the only component that grows with document count
2. Fixed components (cache + base + file cache) are subtracted from the 80% RAM ceiling
3. The remaining headroom is divided by per-document forest memory to estimate capacity
4. Fragment limits (96M per forest) and stand limits (64 per forest) are checked separately
5. Scaling recommendations flag issues: high fragmentation, stand pressure, memory pressure

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

### Trend Analysis

With 2+ snapshots, `--trend` shows:

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
do not grow with document count. **Forest memory** is the only component that
scales with data volume. MLCA subtracts fixed components from the memory ceiling
to determine the true headroom available for forest growth.

## File Structure

```
ml-capacity/
  ml_capacity.py        # Main capacity analyzer CLI
  ml_capacity_test.py   # Scaling validation test
  .ml-capacity/         # Snapshot storage (gitignored)
    20260410T120000_Documents.json
    20260410T130000_Documents.json
    ...
  .gitignore
  README.md
```
