---
title: "Clickstream on Parquet vs Iceberg: A Hands-On Engineering Guide"
date: 2026-02-26
draft: false
tags: ["iceberg", "spark", "clickstream", "data-engineering", "lakehouse"]
categories: ["data-platform"]
description: "A practical side-by-side comparison of Parquet-only tables vs Apache Iceberg for clickstream pipelines at scale."
---

## 1. Why Clickstream Data Needs Modern Table Formats

Almost every product team depends on clickstream data: growth, recommendations, fraud, experimentation, and ML training.
The challenge is not just writing files. The challenge is operating safely at scale with:

- Late-arriving events
- Duplicate retries
- Backfills
- Schema changes
- Partition strategy changes
- Reproducible historical reads

This is where Parquet-only tables and Iceberg diverge.

### Parquet-only (directory + files)

- Good for simple append
- Manual partition rewrites
- No built-in snapshot history
- Harder idempotency and auditability

### Iceberg table format

- Snapshot isolation
- Time travel
- ACID commits
- MERGE-based upserts
- Schema and partition evolution
- Metadata-driven planning (manifests, manifest lists)

This post is hands-on and based on the exact Spark + SQL workflow shown in the code snippets below.

[generate_clickstream_data.py](https://github.com/amitpoorab/iceberg_clickstream/blob/main/scripts/generate_clickstream_data.py)
[late_arrivals_demo.py](https://github.com/amitpoorab/iceberg_clickstream/blob/main/scripts/late_arrivals_demo.py)

---

## 2. Creating an Iceberg Table

In the generator, Spark is configured with Iceberg catalog settings, and tables are written using DataFrameWriterV2 semantics.

A partitioned table is created with an event_date column derived from timestamp and partitioned on that field.

Core idea:

- Compute event_date = to_date(timestamp)
- Create events_by_date with partitionedBy("event_date")
- Append batches safely after table creation

---

## 3. Ingesting 1B Clickstream Events

Generating 1B in a single in-memory list will OOM most local environments.
The correct pattern is batched generation + append:

- Total rows: 1,000,000,000
- Batch size: 10,000,000
- Loop batches:
  - generate batch
  - convert timestamp types
  - write batch
  - clear temporary objects
- Repeat until complete

This keeps memory bounded and writes incremental snapshots.

---

## 4. Snapshot & Manifest Internals: The Heart of Iceberg

This is the most important section to understand before running any Iceberg workload.
Everything else — time travel, late-event correctness, incremental pipelines, compaction safety — depends on how Iceberg manages its metadata layer.

### The 3-level metadata hierarchy

Iceberg never trusts directory listings. All table state is encoded in a tree of metadata files:

```
Table Metadata JSON  (current_snapshot_id = 7249404491565481623)
        │
        ▼
  Snapshot (DELETE correction)
  snapshot_id: 7249404491565481623
  parent_id:   8230528333944786019
  operation:   overwrite
  manifest_list: snap-7249404491565481623.avro
        │
        ▼
  Manifest List  (snap-7249404491565481623.avro)
  ┌────────────────────────────────────────────────────────────────────────────────┐
  │  manifest                      snapshot_owner        added  deleted  partition │
  │  snap-7692...bulk.avro         7692014682867049918    181      0    03-05/03-06│ ← REUSED (bulk load)
  │  snap-6575...late1.avro        6575424270833126431      1      0    2026-02-24 │ ← REUSED
  │  snap-6155...late2.avro        6155764582266208314      1      0    2026-03-05 │ ← REUSED
  │  snap-1423...late3.avro        1423961137926280065      1      0    2026-03-06 │ ← REUSED
  │  snap-8230...late5.avro        8230528333944786019      1      0    2026-02-24 │ ← REUSED
  │  snap-7249...delete-1.avro     7249404491565481623      0      1    2026-02-24 │ ← NEW (delete)
  │  snap-7249...delete-2.avro     7249404491565481623      0      1    2026-02-24 │ ← NEW (delete)
  │  snap-7249...corrected.avro    7249404491565481623      1      0    2026-02-24 │ ← NEW (corrected row)
  └────────────────────────────────────────────────────────────────────────────────┘
           │                         │                        │
           ▼                         ▼                        ▼
  181 Parquet files            tiny late-arrival         1 corrected
  (bulk load, ~245 MB each)    files (0.01 MB each)      Parquet file
```

Key insight: **manifests are immutable and reused across snapshots**.
A new snapshot writes a new manifest list pointing to the new manifests **plus** all previously unchanged manifests.
This is why Iceberg metadata operations are cheap even at billions of rows.

### What is a snapshot?

A snapshot is an atomic, immutable record of the table at a point in time.
Every `append`, `overwrite`, or `delete` operation creates exactly one new snapshot.

Each snapshot records:

| Field | Meaning |
|---|---|
| `snapshot_id` | Unique 64-bit ID for this commit |
| `parent_id` | Previous snapshot (forms a linked chain) |
| `committed_at` | Wall-clock timestamp of commit |
| `operation` | `append`, `overwrite`, `delete`, `replace` |
| `manifest_list` | Path to the Avro file listing all manifests |
| `summary` | Stats map: `added-records`, `total-records`, `added-files`, etc. |

Inspect snapshots directly:

```sql
SELECT
  snapshot_id,
  parent_id,
  committed_at,
  operation,
  summary['added-records']    AS added_records,
  summary['total-records']    AS total_records,
  summary['added-data-files'] AS added_data_files
FROM iceberg.clickstream.events_by_date.snapshots
ORDER BY committed_at DESC
LIMIT 5;
```

Actual output from our 1B-row clickstream table (most recent first):

```
+-------------------+-------------------+-----------------------+---------+--------------+-------------+----------------+
|snapshot_id        |parent_id          |committed_at           |operation|added_records |total_records|added_data_files|
+-------------------+-------------------+-----------------------+---------+--------------+-------------+----------------+
|7249404491565481623|8230528333944786019|2026-03-16 16:08:46.679|overwrite|11            |1000008027   |1               |
|8230528333944786019|649313348689494934 |2026-03-16 16:08:39.672|append   |5             |1000008027   |1               |
|649313348689494934 |6843122336100381253|2026-03-14 21:46:42.921|overwrite|6             |1000008022   |1               |
|6843122336100381253|2502226587015138694|2026-03-14 21:46:37.532|append   |5             |1000008022   |1               |
|2502226587015138694|6575424270833126431|2026-03-14 21:39:29.443|append   |1             |1000008017   |1               |
+-------------------+-------------------+-----------------------+---------+--------------+-------------+----------------+
```

Reading this:
- The latest snapshot (`7249404491565481623`) is an `overwrite` — that is the DELETE correction step for `late-evt-2`.
- The one before it (`8230528333944786019`) is an `append` of 5 late-arriving events.
- Total rows: **1,000,008,027** — the 1B base load plus all late inserts and corrections.
- `added_data_files=1` for each late-arrival commit: each small batch produces exactly 1 tiny Parquet file — the small-file problem in action.

### How the snapshot chain evolves

Here is the real snapshot lineage from our 1B-row clickstream table, reading the `parent_id` chain:

```
[S_bulk: snap=7692014682867049918]  →  ...  →  [S_late_append: snap=8230528333944786019]
  append (1B base load in batches)                append (5 late events)
  181 data files in one bulk manifest             1 new manifest
  partitions 2026-03-05/2026-03-06               partition 2026-02-24

   ↓ parent_id chain

[S_delete: snap=7249404491565481623]
  overwrite (DELETE correction for late-evt-2)
  2 delete-manifest entries + 1 new data file
  2026-02-24 partition
```

Key observation: the `overwrite` operation from the DELETE adds **new deletion manifests** into the manifest list.
The original data files for the deleted row are not physically removed — they are marked `DELETED` in a manifest entry.
Physical removal happens during compaction/expiry.

---

## 5. Time Travel

Time travel lets you query the table at an older snapshot:

```sql
SELECT COUNT(*)
FROM iceberg.clickstream.events_by_date VERSION AS OF 123456789;
```

This is useful for:

- Debugging pipeline regressions
- Audit reproducibility
- ML dataset reproducibility
- Backtest and incident analysis

---

## 6. Handling Late Events

Late events are unavoidable in clickstream systems.

### Parquet-only approach

- Usually partition by event_date
- Late data for old date may require rewrite or complicated dedupe jobs
- Operational risk increases with each correction

### Iceberg approach

- Append late rows
- New snapshot records new files and partition values
- Query planner uses metadata to prune files

Important: dedupe still matters in both systems. Iceberg gives transactional tools to do it safely.

---

## 7. Upserts with MERGE

MERGE is central for:

- CDC pipelines
- Idempotent ingestion
- Retry-safe semantics

Example pattern:

```sql
MERGE INTO iceberg.clickstream.events_by_date t
USING staging_clickstream s
ON t.event_id = s.event_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

Outcome:

- One logical row per key (event_id)
- Cleaner handling of corrections/replays

---

## 8. Schema Evolution

Clickstream payloads evolve constantly (new attributes, deprecated fields, type changes).
Iceberg supports schema evolution without rewriting all historical data files.

Typical operations:

- ADD COLUMN
- RENAME COLUMN
- DROP COLUMN (carefully, with downstream checks)

This reduces migration overhead and downtime risk.

---

## 9. Partition Evolution

One of the hardest operational problems in Parquet-only layouts is changing partition strategy.

Example evolution:

- Start with event_date
- Later need hourly partitioning for higher selectivity
- Or move to transformed partitioning based on timestamp

Iceberg supports partition spec evolution across snapshots.
Old data keeps old spec; new data uses the new spec.

---

## 10. Small File Compaction

Streaming + late arrivals often create small files.
Small files hurt planning and scan efficiency.

With Iceberg:

- Run compaction/rewrite actions
- Produce fewer, larger files
- Keep transactional safety and snapshot lineage

This is a normal maintenance loop in production pipelines.

---

## 11. Manifest Files: What Changes and When

Manifest files are the engine behind Iceberg's performance and correctness.
Every data engineer working with Iceberg at scale should understand what manifests contain, how they evolve, and what queries expose them.

### What a manifest file contains

A manifest is an Avro file. Each row in the manifest describes one data file:

| Column | Meaning |
|---|---|
| `status` | `0=EXISTING`, `1=ADDED`, `2=DELETED` |
| `data_file.file_path` | Full path to the Parquet file |
| `data_file.partition` | Partition values (e.g., `{event_date: 2026-02-24}`) |
| `data_file.record_count` | Row count in this file |
| `data_file.file_size_in_bytes` | Physical size |
| `data_file.column_sizes` | Per-column byte sizes |
| `data_file.value_counts` | Per-column non-null count |
| `data_file.null_value_counts` | Per-column null count |
| `data_file.lower_bounds` | Per-column min value |
| `data_file.upper_bounds` | Per-column max value |

Those lower/upper bounds are how Iceberg prunes files at planning time — **without reading the data files at all**.

### Querying manifests

```sql
-- Manifest-level view: which snapshot wrote each manifest, file counts per partition
SELECT
  added_snapshot_id,
  content,
  added_data_files_count,
  existing_data_files_count,
  deleted_data_files_count,
  partition_summaries
FROM iceberg.clickstream.events_by_date.manifests
ORDER BY added_snapshot_id;
```

Sample output (actual query result from our 1B-row clickstream table):

```
+-------------------+-------+----------------------+-------------------------+------------------------+----------------------------------------+
|added_snapshot_id  |content|added_data_files_count|existing_data_files_count|deleted_data_files_count|partition_summaries                     |
+-------------------+-------+----------------------+-------------------------+------------------------+----------------------------------------+
|7692014682867049918|0      |181                   |0                        |0                       |[{false, false, 2026-03-05, 2026-03-06}]|
|7249404491565481623|0      |1                     |0                        |0                       |[{false, false, 2026-02-24, 2026-02-24}]|
|7249404491565481623|0      |0                     |0                        |1                       |[{false, false, 2026-02-24, 2026-02-24}]|
|7249404491565481623|0      |0                     |0                        |1                       |[{false, false, 2026-02-24, 2026-02-24}]|
|6575424270833126431|0      |1                     |0                        |0                       |[{false, false, 2026-02-24, 2026-02-24}]|
|6155764582266208314|0      |1                     |0                        |0                       |[{false, false, 2026-03-05, 2026-03-05}]|
|1423961137926280065|0      |1                     |0                        |0                       |[{false, false, 2026-03-06, 2026-03-06}]|
+-------------------+-------+----------------------+-------------------------+------------------------+----------------------------------------+
```

Reading this:
- Snapshot `7692014682867049918` owns the big bulk load manifest: **181 data files** covering partitions `2026-03-05` and `2026-03-06`.
- Snapshot `7249404491565481623` (the DELETE correction) owns 3 manifests:
  - 1 manifest with `added_data_files_count=1` — the replacement row
  - 2 manifests with `deleted_data_files_count=1` each — marking old files as deleted
- Each late-arrival append (`6575424270833126431`, `6155764582266208314`, `1423961137926280065`) adds exactly 1 manifest with 1 data file.

Note `added_snapshot_id=7249404491565481623` for the delete correction.
It added 2 deletion manifests and 1 new-file manifest into the `event_date=2026-02-24` partition.
The other manifests from prior bulk batches are completely untouched.

### Querying data files directly

```sql
SELECT
  file_path,
  partition,
  record_count,
  file_size_in_bytes,
  ROUND(file_size_in_bytes / 1024.0 / 1024.0, 2) AS size_mb
FROM iceberg.clickstream.events_by_date.files
ORDER BY partition, record_count DESC;
```

Actual output from our clickstream table:

```
+------------+----------+----------+-------------+-----------+
|partition   |file_count|total_rows|total_size_mb|avg_file_mb|
+------------+----------+----------+-------------+-----------+
|{2026-02-24}|2         |27        |0.01         |0.01       |
|{2026-03-05}|133       |799549359 |32607.21     |245.17     |
|{2026-03-06}|50        |200458641 |7635.11      |152.70     |
+------------+----------+----------+-------------+-----------+
```

This is exactly the small-file problem in the wild:
- `2026-03-05` and `2026-03-06` have healthy avg file sizes of ~245 MB and ~153 MB from the bulk load.
- `2026-02-24` has only **2 files holding 27 rows total** — those are the late-arriving and corrected events.
  They cost 0.01 MB but each one adds its own manifest entry and forces the planner to open them separately.

This is your compaction signal: when `avg_file_mb` for a partition drops far below your target (usually 128–256 MB),
schedule a rewrite/compact job for that partition.

### How the manifest list changes across snapshots

This is the core of Iceberg's efficiency story.
Here is the manifest list state at each key snapshot for our actual clickstream table:

**After bulk load snapshot `7692014682867049918` (~1B base events):**
```
Manifest List  snap-7692014682867049918.avro
└── manifest-bulk.avro   added_snapshot=7692014682867049918   files: 181
                         partition range: [2026-03-05, 2026-03-06]
```

**After first late-arrival append `6575424270833126431` (1 late event, 2026-02-24):**
```
Manifest List  snap-6575424270833126431.avro
├── manifest-bulk.avro        added_snapshot=7692014682867049918   ← REUSED (181 files)
└── manifest-late-1.avro      added_snapshot=6575424270833126431   files: 1
                              partition: [2026-02-24, 2026-02-24]
```

**After late-arrival append `8230528333944786019` (5 more late events, 2026-02-24):**
```
Manifest List  snap-8230528333944786019.avro
├── manifest-bulk.avro        added_snapshot=7692014682867049918   ← REUSED
├── manifest-late-1.avro      added_snapshot=6575424270833126431   ← REUSED
├── manifest-late-2.avro      added_snapshot=6155764582266208314   ← REUSED
├── manifest-late-3.avro      added_snapshot=1423961137926280065   ← REUSED
└── manifest-late-5.avro      added_snapshot=8230528333944786019   files: 1  ← NEW
```

**After DELETE correction snapshot `7249404491565481623` (delete late-evt-2 + insert corrected row):**
```
Manifest List  snap-7249404491565481623.avro
├── manifest-bulk.avro        added_snapshot=7692014682867049918   ← REUSED (181 files)
├── manifest-late-*.avro      added_snapshot=...                   ← REUSED (multiple)
├── manifest-delete-1.avro    added_snapshot=7249404491565481623   deleted_files: 1  ← NEW
├── manifest-delete-2.avro    added_snapshot=7249404491565481623   deleted_files: 1  ← NEW
└── manifest-corrected.avro   added_snapshot=7249404491565481623   added_files: 1    ← NEW
```

**Takeaway:** The 181-file bulk manifest was never touched across any of these commits.
Only tiny new manifests were appended to the list. For unchanged partitions, metadata growth is close to constant per commit.
The bulk manifests from `2026-03-05` / `2026-03-06` are completely unaffected by the `2026-02-24` late arrivals.

### How planners use manifests for file pruning

When you run:

```sql
SELECT COUNT(*) FROM iceberg.clickstream.events_by_date
WHERE event_date = '2026-02-24';
```

Iceberg's planner does:

1. Reads table metadata → finds current snapshot
2. Reads manifest list for current snapshot
3. For each manifest, checks `partition_summaries` — skips manifests where partition can't match
4. For matching manifests, reads manifest rows and applies **column-level min/max pruning**
5. Only opens the Parquet files that survived pruning

This is why Iceberg planning is fast even with thousands of partitions and millions of files — it never lists the directory.

### Late arrivals and manifest growth

Each late-arrival insert appends at least one new manifest.
After many late-arrival batches into the same partition:

```
Manifest List  snap-N.avro
├── manifest-A    (original bulk load, 2026-02-24, 2.5M rows)
├── manifest-D    (batch 2, 2026-02-24, 2.5M rows)
├── manifest-C    (late batch 1, 2026-02-24,     5 rows)  ← tiny file
├── manifest-F    (late batch 2, 2026-02-24,     3 rows)  ← tiny file
├── manifest-G    (late batch 3, 2026-02-24,    12 rows)  ← tiny file
└── ...
```

This manifests (pun intended) in two problems:
1. **Small file proliferation** — many tiny Parquet files per partition
2. **Manifest list growth** — more manifests to scan at planning time

The operational fix is compaction: merge small files, rewrite manifests.
After compaction, the manifest list for that partition collapses back to a few large-file entries.

---

## 12. Incremental Pipelines

Incremental pipelines can process only new snapshots after a checkpoint.

Pattern:

1. Store last processed snapshot ID
2. Read newer snapshot/file metadata
3. Process delta
4. Advance checkpoint

This is cleaner than directory-listing heuristics in Parquet-only systems.

---

## 13. Performance at Scale

At high volume, the win is not only format efficiency; it is operational efficiency:

- Better planning via manifests
- Snapshot-based consistent reads
- Easier backfills and replay
- Safer late-event correction workflows
- Lower engineering overhead for schema/partition change

### Practical takeaway

If your clickstream pipeline is append-only and small, Parquet-only may be enough.
If you need correctness, reproducibility, late-event handling, and safe evolution at scale, Iceberg is the stronger default.

---

## Code Snippets

### Spark + Iceberg session config

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ClickstreamGenerator") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", "/home/jovyan/work/data/iceberg_warehouse") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.defaultCatalog", "iceberg") \
    .getOrCreate()
```

### Batched generation pattern (for large scale)

```python
num_events = 1_000_000_000
batch_size = 10_000_000
num_batches = (num_events + batch_size - 1) // batch_size
events_written = 0

for batch_num in range(num_batches):
    current_batch = min(batch_size, num_events - events_written)
    events = generator.generate_batch(current_batch, batch_date)
    # convert timestamps, create DataFrame, write append
    events_written += current_batch
```

### Partitioned table write

```python
from pyspark.sql import functions as F

df_by_date = df.withColumn("event_date", F.to_date(F.col("timestamp")))
df_by_date.writeTo("iceberg.clickstream.events_by_date") \
    .partitionedBy("event_date") \
    .create()
```

### Append for subsequent batches

```python
df_by_date.writeTo("iceberg.clickstream.events_by_date").append()
```

### Time travel

```sql
SELECT COUNT(*)
FROM iceberg.clickstream.events_by_date VERSION AS OF 123456789;
```

### Late arrivals visibility

```sql
SELECT event_date, COUNT(*)
FROM iceberg.clickstream.events_by_date
GROUP BY event_date
ORDER BY event_date;
```

### MERGE upsert pattern

```sql
MERGE INTO iceberg.clickstream.events_by_date t
USING staging_clickstream s
ON t.event_id = s.event_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

### Snapshot chain inspection

```sql
-- Full snapshot chain with record deltas
-- Note: column is parent_id (not parent_snapshot_id) in Iceberg 1.5+
SELECT
  snapshot_id,
  parent_id,
  committed_at,
  operation,
  summary['added-records']    AS added_records,
  summary['deleted-records']  AS deleted_records,
  summary['total-records']    AS total_records,
  summary['added-data-files'] AS added_files
FROM iceberg.clickstream.events_by_date.snapshots
ORDER BY committed_at DESC;
```

### Manifest list evolution per snapshot

```sql
-- Which manifests belong to the current snapshot, and which are reused vs new
SELECT
  added_snapshot_id,
  content,
  added_data_files_count,
  existing_data_files_count,
  deleted_data_files_count,
  partition_summaries
FROM iceberg.clickstream.events_by_date.manifests
ORDER BY added_snapshot_id;
```

### Data file inspection with size quality signal

```sql
-- Spot small files from late arrivals or streaming
SELECT
  file_path,
  partition,
  record_count,
  file_size_in_bytes,
  ROUND(file_size_in_bytes / 1024.0 / 1024.0, 2) AS size_mb
FROM iceberg.clickstream.events_by_date.files
ORDER BY file_size_in_bytes ASC;
```

### Partition-level record distribution

```sql
-- See how records distribute across partitions (useful after late arrivals)
SELECT
  partition,
  COUNT(*)          AS file_count,
  SUM(record_count) AS total_rows,
  ROUND(SUM(file_size_in_bytes) / 1024.0 / 1024.0, 1) AS total_mb,
  ROUND(AVG(file_size_in_bytes) / 1024.0 / 1024.0, 2) AS avg_file_mb
FROM iceberg.clickstream.events_by_date.files
GROUP BY partition
ORDER BY partition;
```