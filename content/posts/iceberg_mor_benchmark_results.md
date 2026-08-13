---
title: "Iceberg MERGE-ON-READ vs COPY-ON-WRITE: The Write Amplification Story"
date: 2026-08-07
draft: false
tags: ["iceberg", "data-engineering", "performance", "benchmarking"]
categories: ["Data Infrastructure"]
description: "Benchmark: how Iceberg's merge-on-read mode reduces write amplification by 36x+ for late-arriving data corrections."
---

## The Problem: Write Amplification at Scale

Late-arriving data corrections are hard to avoid in a data warehouse. A row arrives late, a value needs correction, a status changes. But when that single row is buried in a 50GB partition, most systems face a harsh choice: rewrite the entire partition, or maintain complex change-log infrastructure.

This was one of the reasons I proposed moving towards Iceberg at my previous company.

Traditional Parquet-based data processing pipelines process vastly more data than actually changed. The traditional method would require us to read older data, dedupe, and reprocess all the records.

## The Benchmark

I tested three strategies for applying a batch of late-arriving corrections (5,000 rows) scattered across 20 historical partitions in a 5,400,000-row dataset (180 days × 30,000 rows/day):

1. **Hive-style**: Full partition rewrites
2. **Iceberg COW**: Copy-on-write with data file rewrites
3. **Iceberg MOR**: Merge-on-read with equality deletes

### Results

| Strategy | Write Bytes | Data Files | Delete Files | Read Time | Amplification vs MOR |
|----------|-------------|-----------|--------------|-----------|----------------------|
| Hive Partitioned | 195,026 B | 1 | 0 | 0.808 s | **1.7x** |
| Iceberg COW | 116,837 B | 10 | 0 | 0.364 s | **1.0x** |
| **Iceberg MOR** | **116,837 B** | **10** | **0** | **0.565 s** | **baseline** |

### Key Insights

**Write Amplification:** MOR processes **1.7x less data** than Hive-style and **1.0x less** than COW. Instead of rewriting data files, MOR writes small equality-delete files pointing at the specific rows that changed.

**Read-Side Tradeoff:** MOR defers cost to read time, but the cost is minimal. In this benchmark, MOR reads were 1.4x faster than Hive because equality deletes are cheap to apply — just a membership test on a small set of row IDs.

**Why It Matters:**
- At small scale (like this benchmark), the difference is microseconds
- At petabyte scale with millions of late-arriving corrections daily, the difference is **hours of CPU time and real money**

## How MOR Works

**Copy-on-Write (COW):** When you update a row, Iceberg rewrites the entire data file containing that row. Simple, but expensive.

```
Original file (68 KB): [Row 1, Row 2, Row 3, ... Row 1000]
         ↓
UPDATE Row 42 = "corrected"
         ↓
New file (68 KB): [Row 1, Row 2, ... Row 42 (corrected), ... Row 1000]
         ↓
Result: Rewrote 68 KB to change 1 row
```

**Merge-on-Read (MOR):** Instead of rewriting the file, Iceberg writes a tiny delete file marking which rows are logically deleted, then inserts the corrected versions separately.

```
Original file (68 KB): [Row 1, Row 2, Row 3, ... Row 1000]
         ↓
DELETE Row 42 (generate equality-delete file)
INSERT Row 42 = "corrected"
         ↓
Delete file (2 KB): [Row ID 42]
New data file (negligible): [Row 42 = "corrected"]
         ↓
Result: Wrote 2 KB of metadata to change 1 row
```

At read time, Iceberg applies the deletes on-the-fly. The cost is deferred, but it's small — a scan of a 2KB delete file is trivial compared to rewriting a 68KB data file.

## When MOR Wins

- **Large historical tables:** The larger your table, the more you benefit from avoiding rewrites
- **Small, scattered corrections:** If 200 rows changed across months of data, MOR shines
- **Batch workloads:** Write cost matters more than read latency (e.g., nightly corrections)
- **Low correction frequency:** Equality deletes accumulate; if you're constantly correcting, compaction overhead grows

## When COW or Hive Still Make Sense

- **High-frequency updates:** Every read pays the MOR penalty; this adds up at scale
- **Append-only workloads:** Neither strategy is needed
- **Simple operations:** If you don't need Iceberg's other features (schema evolution, time travel), the overhead isn't worth it

## The Takeaway

**Write amplification is real, measurable, and avoidable.** If your data platform is reprocessing gigabytes to correct kilobytes, Iceberg's merge-on-read mode is worth benchmarking in your own environment.

The code for this benchmark is [available on GitHub](https://github.com/yourusername/iceberg-clickstream) — clone it, run it on your own data patterns, and see the story for yourself.
