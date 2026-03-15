---
title: "How We Reduced Data Ingestion Time by 70% on Our Data Platform"
date: 2025-11-10T09:00:00Z
slug: "how-we-reduced-data-ingestion-time"
description: "Lessons from building high-throughput, maintainable data lakes with Apache Iceberg."
summary: "Why Apache Iceberg metadata pruning, hidden partitioning, and compaction reduced ingestion time by roughly 70% in a large-scale data platform."
tags:
  - apache-iceberg
  - spark
  - aws-glue
  - data-engineering
draft: false
---

# How We Reduced Data Ingestion Time by 70% on Our Data Platform

*Lessons from building high-throughput, maintainable data lakes*

## TL;DR

As data platforms scale, small files on S3 quietly become one of the biggest performance bottlenecks — they slow ingestion, inflate metadata operations, and make queries painfully sluggish.

By adopting **Apache Iceberg** — primarily for its metadata pruning, hidden partitioning, and compaction capabilities rather than full ACID — we reduced ingestion times by roughly **70%** and dramatically improved query planning, even in a large multi-petabyte environment.

This post explains why Iceberg's architecture delivers these gains, how it changes real ingestion behavior, and the practical lessons from rolling it out in a high-concurrency data lake.

## 1. The Scaling Problem

At small scale, S3-based pipelines feel effortless: write Parquet files, partition by date, run a Glue crawler — everything just works.

Then scale hits.

With many datasets and services writing concurrently, classic symptoms emerge:

- **Slow ingestion** — each Spark task produces tiny files → S3 LIST operations pile up
- **Query planning delays** — Redshift Spectrum / Athena / Spark SQL spends minutes listing partitions
- **High read latency** — opening tens of thousands of small files kills I/O throughput
- **Partition management pain** — manually maintained `year/month/day` folders become messy and error-prone

These start as operational annoyances but compound quickly: ingest jobs miss SLAs, queries that took seconds now take minutes, and engineering time shifts from features to firefighting.

The real challenge wasn't just raw performance — it was **scalability**, **maintainability**, and **predictability** across hundreds of datasets and concurrent writers.

**Goal**: Faster ingestion, predictable queries, simpler data management — without increasing operational burden.

## 2. Evaluating Options: Balancing Performance and Maintainability

Before jumping to a new table format, we asked the hard question:

“What exactly is slowing us down — and can we fix it without adding needless complexity?”

The root cause was cumulative small inefficiencies: partition handling, schema evolution tracking, and Spark ↔ Glue Catalog interaction.

We evaluated three realistic paths:

### 2.1 Staying with Glue DynamicFrames

Convenient, seamless AWS integration, excellent schema drift handling.  
But at scale: DataFrame → DynamicFrame conversion became a serious bottleneck. Since data was already Parquet, the flexibility didn't justify the overhead.

**Verdict**: Great for semi-structured data with heavy evolution, but suboptimal for high-throughput, terabyte-scale ingestion.

### 2.2 Hive-Style Partitions

Classic folder-based partitioning (`year=YYYY/month=MM/day=DD`) + query-engine pruning. Simple and proven.  
Downside: Doesn't age well. Beyond thousands of partitions, S3 eventual consistency + LIST operations dominate. We spent more time listing than reading.

**Verdict**: Fine for small-to-medium scale, unsustainable for hundreds of datasets with concurrent writers.

### 2.3 Apache Iceberg

Iceberg rethinks metadata — the layer most teams ignore until it becomes the bottleneck.

## 3. Iceberg — Why It Works

Iceberg separates **what data exists** from **where it physically lives** via a metadata tree. Instead of slow S3 directory listings or Glue partition scans, it maintains lightweight **manifest files** containing:

- Pointers to Parquet files
- Partition values
- Row counts
- Column min/max statistics

Query engines read a few metadata files → instantly know which data files to scan (or skip).

### Key Building Blocks

1. **Manifests & Manifest Lists**  
   Store file-level statistics → enable metadata-based pruning (skip entire files without touching data).

2. **Snapshots**  
   Every write produces a new immutable snapshot. Parallel reads/writes stay consistent without blocking.

3. **Hidden Partitioning**  
   No manual `year/month/day` folders. Iceberg maps logical partitions (e.g. `ingestion_date`) to physical layout automatically — reduces errors and improves pruning reliability.

4. **Bin-Pack Compaction**  
   `rewrite_data_files` merges small files into ~256–512 MB targets without full-dataset rewrites → fixes small-file problems elegantly.

### Why It Helped Us

We didn't need full ACID — but gained massively from metadata efficiency:

- Ingestion sped up: no partition listing/computation in Spark jobs
- Query latency dropped: engines (Spectrum, Spark SQL, Athena) use manifest pruning instead of folder scans
- Schema evolution simplified: add/rename columns without backfills or partition recreation

Iceberg shifted the problem from *file management* to *metadata management* — enabling linear scaling with volume and concurrency.

## 4. Step-by-Step Implementation

### 4.1 Start Small: Proof-of-Concept

```sql
-- Spark SQL
CREATE TABLE iceberg_demo.events (
  id BIGINT,
  event_type STRING,
  ts TIMESTAMP,
  ingestion_date DATE
)
USING iceberg
PARTITIONED BY (ingestion_date)
LOCATION 's3://my-personal-lake/iceberg/events/';
```

Ingest subset → compare raw Parquet vs. Iceberg performance.

### 4.2 Adjust Ingestion Patterns

**Before** (classic Parquet):

```python
df.write.partitionBy("ingestion_date").parquet("s3://my-personal-lake/raw/events/")
```

**After** (Iceberg via Glue Catalog):

```python
df.writeTo("glue_catalog.demo.events") \
  .partitionedBy("ingestion_date") \
  .append()
```

Iceberg handles physical partitioning automatically — no manual folder logic.

### 4.3 Compaction: The Performance Booster

Schedule daily (or after heavy write windows):

```sql
-- Bin-pack compaction targeting ~512 MB files
CALL glue_catalog.system.rewrite_data_files(
  table => 'demo.events',
  strategy => 'binpack',
  options => map('target-file-size-bytes', '536870912')
);

-- Expire old snapshots (keep metadata lean)
CALL glue_catalog.system.expire_snapshots(
  table => 'demo.events',
  older_than => CURRENT_TIMESTAMP - INTERVAL '30' DAY,
  retain_last => 3
);
```

## 5. What Improved (Rough Observations)

| Metric                     | Before              | After               | Notes                                      |
|----------------------------|---------------------|---------------------|--------------------------------------------|
| Ingestion time (per batch) | High                | ~70–80% lower       | Sandbox → production directional gains     |
| Query planning             | Several seconds     | Sub-second          | Manifest pruning eliminates S3 LIST        |
| File count                 | 1000s               | <100                | After compaction                           |
| Query runtime (P90)        | Slow                | Fast                | Fewer small files + better pruning         |

*Numbers anonymized and directional — real impact varied by workload.*

## 6. Key Takeaways

1. Choose partition keys aligned with **query patterns** (e.g., `ingestion_date` for daily filters)
2. Target **256–512 MiB** files — balances S3 GET costs and parallelism
3. **Schedule compaction** daily or post-burst
4. Run `expire_snapshots` regularly — metadata size matters
5. Test with your engine — Athena/Spectrum often auto-applies partition filters
6. **Start small** — shadow-write to Iceberg for a week before cutover

## 7. Lessons Learned

- Small changes (eliminate conversions, right partitioning) compound across hundreds of pipelines.
- Iceberg enables **repeatable, observable, maintainable** pipelines — not just faster ingestion.
- **Validate incrementally**: one table → measure → expand.

## 8. Final Thoughts

Apache Iceberg is first and foremost a **performance + maintainability** tool — ACID is a bonus.

In large S3 data lakes:

- Small files kill ingestion speed and query planning
- Compaction + manifest-based pruning solves it elegantly
- Test → measure → scale incrementally

Start in a sandbox. Shadow-write. Quantify before full adoption.

Your future self — and your users — will thank you.

*All examples are illustrative, based on public Iceberg patterns and AWS Glue/Spark integration best practices.*
