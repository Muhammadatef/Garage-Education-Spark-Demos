# Garage-Education-Spark-Demos

# 🔥 Apache Spark Job Optimization — The Complete Hands-On Guide

> **From 16 Hours to 2 Hours** — Real-world optimization techniques that reduced an enterprise ETL pipeline runtime by 85%.

This repository contains **12 practical demo cases** covering every major Spark optimization technique. Each case includes a **before/after** comparison with synthetic data generators so you can reproduce the results locally and observe the impact on the **Spark UI**.

---

## 📋 Table of Contents

1. [Prerequisites & Setup](#-prerequisites--setup)
2. [How to Monitor on Spark UI](#-how-to-monitor-on-spark-ui)
3. [Case 01 — Broadcast Join vs Shuffle Join](#case-01--broadcast-join-vs-shuffle-join)
4. [Case 02 — Bucketing for Repeated Joins](#case-02--bucketing-for-repeated-joins)
5. [Case 03 — Partitioning Strategy (Read & Write)](#case-03--partitioning-strategy-read--write)
6. [Case 04 — Salting Skewed Keys](#case-04--salting-skewed-keys)
7. [Case 05 — Coalesce vs Repartition](#case-05--coalesce-vs-repartition)
8. [Case 06 — Data Skewness Detection & Handling](#case-06--data-skewness-detection--handling)
9. [Case 07 — Catalyst Optimizer & Explain Plans](#case-07--catalyst-optimizer--explain-plans)
10. [Case 08 — Compression Codecs (Snappy vs Gzip vs Zstd vs Uncompressed)](#case-08--compression-codecs-snappy-vs-gzip-vs-zstd-vs-uncompressed)
11. [Case 09 — Adaptive Query Execution (AQE)](#case-09--adaptive-query-execution-aqe)
12. [Case 10 — Persist, Cache & Unpersist Strategy](#case-10--persist-cache--unpersist-strategy)
13. [Case 11 — Shuffle Partition Tuning](#case-11--shuffle-partition-tuning)
14. [Case 12 — GC Tuning & Memory Management](#case-12--gc-tuning--memory-management)
15. [Bonus — The spark-submit Cheat Sheet](#bonus--the-spark-submit-cheat-sheet)

---

## 🛠 Prerequisites & Setup

### Environment

```bash
# Tested on
Apache Spark 3.x (local or YARN cluster)
Python 3.9+
Java 11+
```

### Synthetic Data Generator

Every case relies on a shared data generator. Create it once and reuse across demos.

```python
# data_generator.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import random

def get_spark(app_name="SparkOptDemo", configs=None):
    """Create a SparkSession with optional config overrides."""
    builder = SparkSession.builder.appName(app_name).master("local[*]")
    if configs:
        for k, v in configs.items():
            builder = builder.config(k, v)
    return builder.getOrCreate()

def generate_large_table(spark, num_rows=10_000_000, num_partitions=200):
    """Simulate a protocol-events-style table (large fact table)."""
    return (
        spark.range(0, num_rows, numPartitions=num_partitions)
        .withColumn("account_number", (F.col("id") % 500_000).cast("string"))
        .withColumn("event_date", F.date_add(F.lit("2025-01-01"), (F.col("id") % 90).cast("int")))
        .withColumn("session_duration", (F.rand() * 3600).cast("int"))
        .withColumn("download_bytes", (F.rand() * 1_000_000).cast("long"))
        .withColumn("upload_bytes", (F.rand() * 500_000).cast("long"))
        .withColumn("category", F.concat(F.lit("cat_"), (F.col("id") % 50).cast("string")))
    )

def generate_small_lookup(spark, num_rows=5_000):
    """Simulate a small dimension / lookup table (e.g., URL categories)."""
    return (
        spark.range(0, num_rows)
        .withColumn("category", F.concat(F.lit("cat_"), F.col("id").cast("string")))
        .withColumn("category_name", F.concat(F.lit("Category "), F.col("id").cast("string")))
        .withColumn("priority", (F.rand() * 10).cast("int"))
    )

def generate_skewed_table(spark, num_rows=10_000_000, hot_keys=5, hot_key_ratio=0.6):
    """
    Generate a table where a few 'hot' keys hold 60% of the data.
    This simulates real-world data skew (e.g., a few heavy-usage accounts).
    """
    hot_rows = int(num_rows * hot_key_ratio)
    normal_rows = num_rows - hot_rows

    hot_df = (
        spark.range(0, hot_rows)
        .withColumn("account_number", (F.col("id") % hot_keys).cast("string"))
        .withColumn("value", F.rand() * 1000)
    )
    normal_df = (
        spark.range(0, normal_rows)
        .withColumn("account_number", (F.col("id") % 100_000 + hot_keys).cast("string"))
        .withColumn("value", F.rand() * 1000)
    )
    return hot_df.union(normal_df)
```

---

## 📊 How to Monitor on Spark UI

This is the **most important skill** before diving into optimizations. If you can't read the Spark UI, you can't diagnose bottlenecks.

### Accessing the Spark UI

| Mode | URL |
|------|-----|
| Local | `http://localhost:4040` |
| YARN (running) | `http://<resource-manager>:8088` → click Application → Tracking URL |
| YARN (completed) | `http://<history-server>:18080` |
| Databricks | Cluster → Spark UI tab |

### What to Look At — Tab by Tab

#### 1. Jobs Tab
- Shows all **actions** (`.count()`, `.write()`, `.collect()`) and how long each took.
- **Red flag**: One job taking 10x longer than others → likely data skew or a bad join.

#### 2. Stages Tab (⭐ Most Important)
- Each job breaks into stages separated by **shuffles**.
- Click a stage to see **task-level metrics**.
- **Key metrics to watch**:
  - **Task Duration (min / median / max)**: If max >> median, you have **data skew**.
  - **Shuffle Read/Write**: High shuffle = expensive wide transformation.
  - **GC Time**: If > 10% of task time, you have a **memory problem**.
  - **Input Size**: Tells you how much data each task processed.

#### 3. SQL Tab (⭐ For DataFrame/SQL Users)
- Shows the **physical plan** as a visual DAG.
- Look for:
  - `BroadcastHashJoin` vs `SortMergeJoin` — tells you which join strategy Spark chose.
  - `Exchange` nodes — these are **shuffles** (expensive).
  - `WholeStageCodegen` — good, means Tungsten is generating optimized bytecode.
  - `Filter` pushed into `Scan` — means **predicate pushdown** is working.

#### 4. Storage Tab
- Shows what's **cached/persisted** and how much memory it uses.
- **Red flag**: If nothing is cached but the same DataFrame is used 3x, you're recomputing it.

#### 5. Executors Tab
- Shows memory usage, GC time, and task distribution per executor.
- **Red flag**: One executor doing all the work → data skew or bad partitioning.

#### 6. Environment Tab
- Verify your Spark configs actually took effect (e.g., `spark.sql.shuffle.partitions`).

### Quick Diagnostic Checklist

```
[ ] Are any tasks taking 10x longer than the median?  → Data Skew (Case 04, 06)
[ ] Is shuffle read/write massive?                     → Bad join strategy (Case 01, 02)
[ ] Is GC time > 10% of task duration?                 → Memory issue (Case 12)
[ ] Are there thousands of tiny tasks?                  → Too many partitions (Case 05, 11)
[ ] Is the Exchange node enormous?                      → Needs broadcast join (Case 01)
[ ] Same stage appearing multiple times?                → Missing cache/persist (Case 10)
```

### Screenshot Guide for Demo

```
When recording your demo, show the UI in this order:
1. Jobs tab    → "The overall pipeline took X minutes"
2. SQL tab     → "Notice this SortMergeJoin — that's our bottleneck"
3. Stages tab  → Click the slow stage → "Look at the task distribution"
4. Stage Detail → Sort by Duration desc → "This task took 45 min while others took 2 min"
5. Apply fix   → Re-run → Compare side by side
```

---

## Case 01 — Broadcast Join vs Shuffle Join

### The Problem
When joining a large fact table (millions/billions of rows) with a small lookup table, Spark defaults to a **SortMergeJoin** which shuffles BOTH tables across the network. This is extremely wasteful when one table is small enough to fit in memory.

### Real-World Context
At our Telecom ETL pipeline, we were joining 87GB of protocol events with a 50MB URL-category mapping table. Spark shuffled both, causing 87GB+ of network I/O for every run.

### Demo Setup

```python
# case_01_broadcast_join.py
from pyspark.sql import functions as F
from data_generator import get_spark, generate_large_table, generate_small_lookup
import time

spark = get_spark("Case01_BroadcastJoin")

# Generate data
large_df = generate_large_table(spark, num_rows=20_000_000)  # ~20M rows
small_df = generate_small_lookup(spark, num_rows=5_000)       # ~5K rows

large_df.cache().count()
small_df.cache().count()

# =============================================
# ❌ BAD: Default SortMergeJoin (shuffle both)
# =============================================
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # disable auto-broadcast

start = time.time()
result_bad = large_df.join(small_df, "category", "inner")
result_bad.write.mode("overwrite").parquet("/tmp/case01_bad")
print(f"❌ SortMergeJoin: {time.time() - start:.1f}s")

# =============================================
# ✅ GOOD: Explicit Broadcast Join
# =============================================
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50MB")  # re-enable

start = time.time()
result_good = large_df.join(F.broadcast(small_df), "category", "inner")
result_good.write.mode("overwrite").parquet("/tmp/case01_good")
print(f"✅ BroadcastJoin: {time.time() - start:.1f}s")

# Show the execution plans
print("\n--- BAD PLAN ---")
result_bad.explain(True)
print("\n--- GOOD PLAN ---")
result_good.explain(True)
```

### What to Look for on Spark UI

| Tab | Bad (SortMergeJoin) | Good (BroadcastHashJoin) |
|-----|---------------------|--------------------------|
| **SQL Tab** | `Exchange` on **both** sides + `SortMergeJoin` | `BroadcastExchange` on small side only + `BroadcastHashJoin` |
| **Stages** | 3 stages (2 shuffles + 1 join) | 2 stages (1 broadcast + 1 join) |
| **Shuffle Read** | Huge (GBs) | Minimal |
| **Duration** | 3-5x slower | Baseline |

### Rules of Thumb

```
Small table < 10MB   → Always broadcast (auto or explicit)
Small table 10-200MB → Broadcast if executor memory allows
Small table > 200MB  → DON'T broadcast — use bucketing (Case 02) instead
```

### When NOT to Broadcast
- The "small" table is actually 3GB (like our URL events at e&) — use **bucketed joins** instead.
- You're doing a `LEFT JOIN` where the large side is the left table and the small table has NULLs — broadcast still works but test memory.

---

## Case 02 — Bucketing for Repeated Joins

### The Problem
When two large tables are joined repeatedly (e.g., daily batch pipeline), Spark re-shuffles them every single run. Bucketing **pre-sorts and pre-partitions** the data on disk so that joins become shuffle-free.

### Real-World Context
Our pipeline joins protocol events (87GB) with URL events (3GB) on `account_number` every day. Too large for broadcast, so we bucketed both tables on the join key with 400 buckets — reducing join time by 60%.

### Demo Setup

```python
# case_02_bucketing.py
from pyspark.sql import functions as F
from data_generator import get_spark, generate_large_table
import time

spark = get_spark("Case02_Bucketing", {
    "spark.sql.sources.bucketing.enabled": "true",
    "spark.sql.autoBroadcastJoinThreshold": "-1"  # force sort-merge to see the difference
})

# Generate two large tables
events_df = generate_large_table(spark, num_rows=10_000_000)
sessions_df = (
    generate_large_table(spark, num_rows=5_000_000)
    .withColumnRenamed("session_duration", "total_session_time")
    .select("account_number", "total_session_time", "event_date")
)

# =============================================
# ❌ BAD: Join without bucketing (full shuffle)
# =============================================
events_df.write.mode("overwrite").parquet("/tmp/events_no_bucket")
sessions_df.write.mode("overwrite").parquet("/tmp/sessions_no_bucket")

events_nb = spark.read.parquet("/tmp/events_no_bucket")
sessions_nb = spark.read.parquet("/tmp/sessions_no_bucket")

start = time.time()
result_bad = events_nb.join(sessions_nb, "account_number", "inner")
result_bad.write.mode("overwrite").parquet("/tmp/case02_bad")
print(f"❌ Unbucketed join: {time.time() - start:.1f}s")

# =============================================
# ✅ GOOD: Write bucketed tables then join
# =============================================
NUM_BUCKETS = 64

(events_df
 .write
 .mode("overwrite")
 .bucketBy(NUM_BUCKETS, "account_number")
 .sortBy("account_number")
 .saveAsTable("events_bucketed"))

(sessions_df
 .write
 .mode("overwrite")
 .bucketBy(NUM_BUCKETS, "account_number")
 .sortBy("account_number")
 .saveAsTable("sessions_bucketed"))

events_b = spark.table("events_bucketed")
sessions_b = spark.table("sessions_bucketed")

start = time.time()
result_good = events_b.join(sessions_b, "account_number", "inner")
result_good.write.mode("overwrite").parquet("/tmp/case02_good")
print(f"✅ Bucketed join: {time.time() - start:.1f}s")
```

### What to Look for on Spark UI

| Metric | Without Bucketing | With Bucketing |
|--------|-------------------|----------------|
| **Shuffle Read** | GBs | **0 bytes** |
| **Exchange nodes** | 2 (both sides) | **0** |
| **Join type in SQL tab** | `SortMergeJoin` with `Exchange` | `SortMergeJoin` **without** `Exchange` |
| **Number of stages** | 3 | 1 |

### Important Notes

```
- Both tables MUST be bucketed with the SAME number of buckets
- Must use .saveAsTable() — bucketing info is stored in the Hive metastore
- Bucketing is a ONE-TIME write cost that pays off on EVERY subsequent join
- Best for dimension tables that don't change often
- Use 2^n buckets (32, 64, 128, 256, 512) for even distribution
```

---

## Case 03 — Partitioning Strategy (Read & Write)

### The Problem
Without proper disk partitioning, Spark scans the **entire dataset** even when your query only needs one day's data. With partitioning, Spark uses **partition pruning** to skip irrelevant files entirely.

### Demo Setup

```python
# case_03_partitioning.py
from pyspark.sql import functions as F
from data_generator import get_spark, generate_large_table
import time

spark = get_spark("Case03_Partitioning")

df = generate_large_table(spark, num_rows=10_000_000)

# =============================================
# ❌ BAD: Write without partitioning
# =============================================
df.write.mode("overwrite").parquet("/tmp/events_no_partition")

start = time.time()
result_bad = (
    spark.read.parquet("/tmp/events_no_partition")
    .filter(F.col("event_date") == "2025-01-15")
    .agg(F.sum("download_bytes"))
)
result_bad.show()
print(f"❌ Full scan: {time.time() - start:.1f}s")

# =============================================
# ✅ GOOD: Write partitioned by event_date
# =============================================
df.write.mode("overwrite").partitionBy("event_date").parquet("/tmp/events_partitioned")

start = time.time()
result_good = (
    spark.read.parquet("/tmp/events_partitioned")
    .filter(F.col("event_date") == "2025-01-15")
    .agg(F.sum("download_bytes"))
)
result_good.show()
print(f"✅ Partition pruning: {time.time() - start:.1f}s")
```

### What to Look for on Spark UI

| Metric | Without Partitioning | With Partitioning |
|--------|---------------------|-------------------|
| **SQL Tab → Scan node** | `number of files read: ALL` | `number of files read: 1 partition` |
| **Input Size** | Full dataset size | Only filtered partition size |
| **Stages** | Scans everything | Prunes at read time |

### Partitioning Best Practices

```
✅ DO partition by:
   - Date columns (event_date, created_date) → most common and effective
   - Low-cardinality columns (country, region, status)
   - Columns frequently used in WHERE / filter clauses

❌ DON'T partition by:
   - High-cardinality columns (user_id, account_number) → millions of tiny files
   - Columns not used in filters → wasted directory overhead
   
⚠️ WATCH OUT:
   - Too many partitions = small file problem (< 128MB per file is bad)
   - Too few partitions = no pruning benefit
   - Ideal: 128MB — 1GB per partition file
```

---

## Case 04 — Salting Skewed Keys

### The Problem
When a few keys hold a disproportionate amount of data (e.g., 5 accounts generate 60% of all events), the tasks processing those keys become **stragglers** — they take 10-50x longer than others, and the entire job waits for them.

### Real-World Context
In our telecom pipeline, a handful of enterprise accounts generated millions of events each while most accounts had a few hundred. The `GROUP BY account_number` stage had tasks running for 1.4 hours while the median was seconds.

### Demo Setup

```python
# case_04_salting.py
from pyspark.sql import functions as F
from data_generator import get_spark, generate_skewed_table
import time

spark = get_spark("Case04_Salting")

skewed_df = generate_skewed_table(spark, num_rows=20_000_000, hot_keys=5, hot_key_ratio=0.6)
skewed_df.cache().count()

# Show the skew
print("📊 Data Distribution (top 10 keys by count):")
skewed_df.groupBy("account_number").count().orderBy(F.desc("count")).show(10)

# =============================================
# ❌ BAD: Aggregate on skewed key directly
# =============================================
start = time.time()
result_bad = (
    skewed_df
    .groupBy("account_number")
    .agg(
        F.sum("value").alias("total_value"),
        F.count("*").alias("event_count")
    )
)
result_bad.write.mode("overwrite").parquet("/tmp/case04_bad")
print(f"❌ Direct aggregation: {time.time() - start:.1f}s")

# =============================================
# ✅ GOOD: Salt the key → aggregate → unsalt
# =============================================
NUM_SALTS = 10

start = time.time()

# Step 1: Add a random salt to spread hot keys across partitions
salted_df = skewed_df.withColumn("salt", (F.rand() * NUM_SALTS).cast("int"))

# Step 2: Aggregate with the salted key (distributes hot keys across 10 tasks)
partial_agg = (
    salted_df
    .groupBy("account_number", "salt")
    .agg(
        F.sum("value").alias("partial_sum"),
        F.count("*").alias("partial_count")
    )
)

# Step 3: Remove salt and do final aggregation
result_good = (
    partial_agg
    .groupBy("account_number")
    .agg(
        F.sum("partial_sum").alias("total_value"),
        F.sum("partial_count").alias("event_count")
    )
)
result_good.write.mode("overwrite").parquet("/tmp/case04_good")
print(f"✅ Salted aggregation: {time.time() - start:.1f}s")
```

### What to Look for on Spark UI

| Metric | Without Salting | With Salting |
|--------|----------------|--------------|
| **Stage Detail → Task Duration** | min=0.1s, max=120s (massive skew) | min=0.5s, max=5s (even) |
| **Stages** | 1 aggregation stage | 2 stages (partial + final) |
| **Straggler tasks** | 1-5 tasks running forever | All tasks finish ~same time |

> 💡 **Demo Tip**: In the Stage Detail view, sort tasks by Duration DESC. Show the audience the skew visually — one bar towering over the rest. Then after salting, show how all bars are roughly equal.

---

## Case 05 — Coalesce vs Repartition

### The Problem
`coalesce(N)` and `repartition(N)` both change the number of partitions, but they behave very differently. Using the wrong one causes either a **full shuffle** when you didn't need it, or **uneven partitions** that defeat the purpose.

### Demo Setup

```python
# case_05_coalesce_vs_repartition.py
from pyspark.sql import functions as F
from data_generator import get_spark, generate_large_table
import time

spark = get_spark("Case05_CoalesceVsRepartition")

df = generate_large_table(spark, num_rows=10_000_000, num_partitions=200)

# =============================================
# Scenario A: Reduce partitions (200 → 10)
# =============================================

# ✅ coalesce — no shuffle, just merges existing partitions
start = time.time()
df.coalesce(10).write.mode("overwrite").parquet("/tmp/case05_coalesce_reduce")
print(f"✅ coalesce(10):     {time.time() - start:.1f}s")

# ⚠️ repartition — full shuffle (unnecessary for reduction)
start = time.time()
df.repartition(10).write.mode("overwrite").parquet("/tmp/case05_repartition_reduce")
print(f"⚠️ repartition(10):  {time.time() - start:.1f}s")

# =============================================
# Scenario B: Increase partitions (200 → 500)
# =============================================

# ❌ coalesce — CANNOT increase partitions effectively (stays at 200)
print(f"\ndf has {df.rdd.getNumPartitions()} partitions")
coalesced = df.coalesce(500)
print(f"coalesce(500) actually has {coalesced.rdd.getNumPartitions()} partitions ← didn't increase!")

# ✅ repartition — proper shuffle to increase
repartitioned = df.repartition(500)
print(f"repartition(500) has {repartitioned.rdd.getNumPartitions()} partitions ← correct!")

# =============================================
# Scenario C: Repartition by a column (for optimized writes)
# =============================================
start = time.time()
df.repartition("event_date").write.mode("overwrite").parquet("/tmp/case05_repartition_col")
print(f"\nrepartition('event_date'): {time.time() - start:.1f}s")

# =============================================
# Scenario D: Check partition sizes (skew detector)
# =============================================
print("\n📊 Partition Size Analysis:")
from pyspark.sql import Row

def show_partition_sizes(dataframe, label):
    sizes = dataframe.rdd.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()
    print(f"\n{label}:")
    print(f"  Partitions: {len(sizes)}")
    print(f"  Min rows:   {min(sizes)}")
    print(f"  Max rows:   {max(sizes)}")
    print(f"  Avg rows:   {sum(sizes)//len(sizes)}")
    print(f"  Skew ratio: {max(sizes) / max(min(sizes), 1):.1f}x")

show_partition_sizes(df.coalesce(10), "coalesce(10)")
show_partition_sizes(df.repartition(10), "repartition(10)")
```

### Cheat Sheet

```
REDUCING partitions:
  ✅ coalesce(N)    — No shuffle, fast. BUT partitions may be uneven.
  ⚠️ repartition(N) — Full shuffle, slower. BUT partitions are perfectly even.

INCREASING partitions:
  ❌ coalesce(N)    — CANNOT increase. Silently does nothing.
  ✅ repartition(N) — Must use this.

WRITING output files:
  ✅ .coalesce(N).write.parquet(...)         — Control number of output files
  ✅ .repartition("col").write.parquet(...)  — One directory per unique value
  
  Our pipeline: .coalesce(180) → 180 output files × ~128MB each = optimal HDFS file sizes
```

### What to Look for on Spark UI

| Operation | Shuffle? | Exchange in SQL plan? | Stages added? |
|-----------|----------|----------------------|---------------|
| `coalesce(10)` | No | No | 0 |
| `repartition(10)` | Yes | Yes | 1 |
| `repartition("col")` | Yes | `hashpartitioning(col)` | 1 |

---

## Case 06 — Data Skewness Detection & Handling

### The Problem
Data skew is the **#1 silent killer** of Spark jobs. Everything looks fine in the plan, but one task takes 100x longer because it got all the hot keys.

### Demo Setup

```python
# case_06_data_skewness.py
from pyspark.sql import functions as F
from data_generator import get_spark, generate_skewed_table
import time

spark = get_spark("Case06_DataSkewness", {
    "spark.sql.adaptive.enabled": "false"  # disable AQE first to see the raw problem
})

# =============================================
# STEP 1: Detect Skew
# =============================================
df = generate_skewed_table(spark, num_rows=20_000_000, hot_keys=3, hot_key_ratio=0.7)

# Method 1: Key frequency analysis
print("🔍 Method 1: Top keys by row count")
key_stats = (
    df.groupBy("account_number")
    .count()
    .orderBy(F.desc("count"))
)
key_stats.show(10)

# Method 2: Percentile analysis
print("🔍 Method 2: Percentile analysis")
key_counts = df.groupBy("account_number").count()
key_counts.select(
    F.expr("percentile_approx(count, 0.5)").alias("p50_median"),
    F.expr("percentile_approx(count, 0.75)").alias("p75"),
    F.expr("percentile_approx(count, 0.95)").alias("p95"),
    F.expr("percentile_approx(count, 0.99)").alias("p99"),
    F.max("count").alias("max"),
    F.min("count").alias("min"),
).show()

# Method 3: Partition-level skew check
print("🔍 Method 3: Partition size distribution")
partition_sizes = df.rdd.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()
print(f"  Min partition:  {min(partition_sizes):,} rows")
print(f"  Max partition:  {max(partition_sizes):,} rows")
print(f"  Skew ratio:     {max(partition_sizes)/max(min(partition_sizes),1):.1f}x")

# =============================================
# STEP 2: Fix with AQE Skew Join
# =============================================
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "64MB")

# Create a second table to join with
df2 = df.groupBy("account_number").agg(F.avg("value").alias("avg_value"))

start = time.time()
result = df.join(df2, "account_number", "inner")
result.write.mode("overwrite").parquet("/tmp/case06_aqe_skew")
print(f"\n✅ AQE Skew Join: {time.time() - start:.1f}s")

# =============================================
# STEP 3: Fix with manual salting for joins
# =============================================
spark.conf.set("spark.sql.adaptive.enabled", "false")  # disable to show manual fix

SALT_BUCKETS = 10

# Salt the large side
salted_large = df.withColumn("salt", (F.rand() * SALT_BUCKETS).cast("int"))

# Explode the small side (replicate it for each salt value)
salted_small = (
    df2
    .crossJoin(spark.range(0, SALT_BUCKETS).withColumnRenamed("id", "salt"))
)

start = time.time()
result_salted = salted_large.join(
    salted_small,
    ["account_number", "salt"],
    "inner"
).drop("salt")
result_salted.write.mode("overwrite").parquet("/tmp/case06_salted_join")
print(f"✅ Salted Join:   {time.time() - start:.1f}s")
```

### What to Look for on Spark UI

```
Stage Detail → Summary Metrics:
┌─────────┬─────────┬──────────┬─────────┬─────────┐
│         │   Min   │  Median  │   75th  │   Max   │
├─────────┼─────────┼──────────┼─────────┼─────────┤
│ SKEWED  │  0.1s   │   2s     │   5s    │  120s   │  ← max/median = 60x SKEW!
│ FIXED   │  0.8s   │   2s     │   3s    │   5s    │  ← max/median = 2.5x OK!
└─────────┴─────────┴──────────┴─────────┴─────────┘
```

> 💡 **Demo Tip**: The Task Duration histogram in the Stage Detail view is the best visual. Show the "long tail" bar chart before the fix, then the uniform distribution after.

---

## Case 07 — Catalyst Optimizer & Explain Plans

### The Problem
Spark's **Catalyst Optimizer** rewrites your logical plan into an optimized physical plan. Understanding it helps you write queries that Spark can optimize, and avoid patterns that block optimization.

### Demo Setup

```python
# case_07_catalyst_optimizer.py
from pyspark.sql import functions as F
from data_generator import get_spark, generate_large_table, generate_small_lookup

spark = get_spark("Case07_CatalystOptimizer")

df = generate_large_table(spark, num_rows=5_000_000)
lookup = generate_small_lookup(spark, num_rows=100)

# =============================================
# Demo 1: Predicate Pushdown
# =============================================
print("=" * 60)
print("DEMO 1: Predicate Pushdown")
print("=" * 60)

# Write as parquet first
df.write.mode("overwrite").parquet("/tmp/case07_events")

# ✅ Filter AFTER read — Catalyst pushes it down INTO the scan
result = (
    spark.read.parquet("/tmp/case07_events")
    .filter(F.col("event_date") == "2025-01-15")
    .filter(F.col("download_bytes") > 500000)
    .select("account_number", "download_bytes")
)
print("\n✅ Predicate Pushdown Plan:")
result.explain(True)
# Look for: PushedFilters in the Scan node

# =============================================
# Demo 2: Projection Pruning (Column Pruning)
# =============================================
print("=" * 60)
print("DEMO 2: Projection Pruning")
print("=" * 60)

# ✅ Even if you SELECT * first, Catalyst prunes unused columns
result2 = (
    spark.read.parquet("/tmp/case07_events")
    .select("account_number", "download_bytes")
    .filter(F.col("download_bytes") > 100000)
)
print("\n✅ Column Pruning Plan:")
result2.explain(True)
# Look for: ReadSchema only includes the 2 columns, not all 7

# =============================================
# Demo 3: Constant Folding
# =============================================
print("=" * 60)
print("DEMO 3: Constant Folding")
print("=" * 60)

# Catalyst folds constant expressions at plan time
result3 = df.filter(F.col("download_bytes") > F.lit(1000) * F.lit(500))
print("\n✅ Constant Folding Plan:")
result3.explain(True)
# Look for: Filter (download_bytes > 500000) — constants pre-computed

# =============================================
# Demo 4: Join Reordering
# =============================================
print("=" * 60)
print("DEMO 4: Auto Broadcast Detection")
print("=" * 60)

# Catalyst automatically detects the small table and broadcasts it
result4 = df.join(lookup, "category", "inner")
print("\n✅ Auto Broadcast Plan:")
result4.explain(True)
# Look for: BroadcastHashJoin (Catalyst chose this automatically)

# =============================================
# Demo 5: What Blocks Catalyst?
# =============================================
print("=" * 60)
print("DEMO 5: Things That Block Optimization")
print("=" * 60)

# ❌ UDFs block Catalyst — it can't see inside them
from pyspark.sql.types import BooleanType
bad_filter = F.udf(lambda x: x > 500000, BooleanType())

result_bad = (
    spark.read.parquet("/tmp/case07_events")
    .filter(bad_filter(F.col("download_bytes")))
)
print("\n❌ UDF blocks pushdown:")
result_bad.explain(True)
# Look for: NO PushedFilters — the UDF prevented pushdown

# ✅ Use built-in functions instead
result_good = (
    spark.read.parquet("/tmp/case07_events")
    .filter(F.col("download_bytes") > 500000)
)
print("\n✅ Built-in function allows pushdown:")
result_good.explain(True)
```

### Reading the Explain Plan (4 Levels)

```
df.explain(True) shows:

1. Parsed Logical Plan   — Raw translation of your code
2. Analyzed Logical Plan  — Resolved column names and types
3. Optimized Logical Plan — AFTER Catalyst applies rules (pushdown, pruning, folding)
4. Physical Plan          — The actual execution plan (join strategies, scan types)

Key things to look for in Physical Plan:
  ✅ PushedFilters: [IsNotNull(col), GreaterThan(col,500000)]  → filter pushed to data source
  ✅ ReadSchema: struct<account_number:string,download_bytes:bigint>  → only needed columns read
  ✅ BroadcastHashJoin  → efficient join strategy chosen
  ❌ No PushedFilters after UDF  → Catalyst couldn't optimize
```

---

## Case 08 — Compression Codecs (Snappy vs Gzip vs Zstd vs Uncompressed)

### The Problem
The compression codec you choose affects **file size**, **read speed**, and **write speed**. There's no single best codec — it depends on whether your bottleneck is I/O or CPU.

### Demo Setup

```python
# case_08_compression.py
from pyspark.sql import functions as F
from data_generator import get_spark, generate_large_table
import time, os, subprocess

spark = get_spark("Case08_Compression")

df = generate_large_table(spark, num_rows=10_000_000)
df.cache().count()

codecs = {
    "none":   "none",
    "snappy": "snappy",
    "gzip":   "gzip",
    "zstd":   "zstd",
    "lz4":    "lz4",
}

results = []

for name, codec in codecs.items():
    path = f"/tmp/case08_{name}"
    
    # Write
    start = time.time()
    df.write.mode("overwrite").option("compression", codec).parquet(path)
    write_time = time.time() - start
    
    # Read
    start = time.time()
    spark.read.parquet(path).count()
    read_time = time.time() - start
    
    # Size on disk
    size_result = subprocess.run(["du", "-sh", path], capture_output=True, text=True)
    size = size_result.stdout.split()[0]
    
    results.append((name, write_time, read_time, size))
    print(f"{name:>10}: write={write_time:.1f}s  read={read_time:.1f}s  size={size}")

# Print summary table
print("\n" + "=" * 65)
print(f"{'Codec':>10} | {'Write (s)':>10} | {'Read (s)':>10} | {'Size':>10}")
print("-" * 65)
for name, wt, rt, sz in results:
    print(f"{name:>10} | {wt:>10.1f} | {rt:>10.1f} | {sz:>10}")
```

### Expected Results (Approximate)

```
    Codec |  Write (s) |   Read (s) |       Size
-----------------------------------------------------------------
     none |        3.2 |        2.1 |       850M    ← fastest write, largest
   snappy |        3.8 |        2.3 |       280M    ← best balance ⭐
     gzip |        8.5 |        3.0 |       180M    ← smallest, slowest write
     zstd |        5.2 |        2.4 |       200M    ← good balance, newer
      lz4 |        3.5 |        2.2 |       310M    ← fast, slightly larger
```

### Recommendation

```
Hot data (read frequently)    → Snappy (default, splittable, fast decompression)
Cold data / archival          → Gzip or Zstd (smaller files, slower)
Network-bound (cloud/S3)      → Zstd (best compression ratio with decent speed)
Local SSD / fast storage      → Snappy or LZ4 (CPU is bottleneck, not I/O)
```

---

## Case 09 — Adaptive Query Execution (AQE)

### The Problem
Before AQE, you had to guess the right `spark.sql.shuffle.partitions` and hope for no skew. AQE dynamically adjusts partitions, optimizes joins, and handles skew **at runtime**.

### Demo Setup

```python
# case_09_aqe.py
from pyspark.sql import functions as F
from data_generator import get_spark, generate_large_table, generate_small_lookup, generate_skewed_table
import time

# =============================================
# Feature 1: Auto Coalesce Shuffle Partitions
# =============================================
print("=" * 60)
print("FEATURE 1: Auto Coalesce Shuffle Partitions")
print("=" * 60)

# Without AQE: 200 shuffle partitions, most nearly empty
spark_no_aqe = get_spark("Case09_NoAQE", {
    "spark.sql.adaptive.enabled": "false",
    "spark.sql.shuffle.partitions": "200"
})

df = generate_large_table(spark_no_aqe, num_rows=1_000_000)

start = time.time()
result = df.groupBy("category").agg(F.sum("download_bytes"))
result.write.mode("overwrite").parquet("/tmp/case09_no_aqe")
print(f"❌ Without AQE: {time.time() - start:.1f}s (200 shuffle partitions)")
spark_no_aqe.stop()

# With AQE: automatically coalesces 200 → ~50 partitions
spark_aqe = get_spark("Case09_WithAQE", {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64MB"
})

df = generate_large_table(spark_aqe, num_rows=1_000_000)

start = time.time()
result = df.groupBy("category").agg(F.sum("download_bytes"))
result.write.mode("overwrite").parquet("/tmp/case09_with_aqe")
print(f"✅ With AQE:    {time.time() - start:.1f}s (auto-coalesced)")

# =============================================
# Feature 2: Dynamic Join Strategy Switch
# =============================================
print("\n" + "=" * 60)
print("FEATURE 2: Dynamic Join Strategy Switch")
print("=" * 60)

# AQE can switch from SortMergeJoin to BroadcastHashJoin
# if it discovers at runtime that one side is small enough
small = df.filter(F.col("category") == "cat_1")  # runtime-small table

result2 = df.join(small, "account_number")
result2.explain(True)  # Check if AQE switched the join strategy

# =============================================
# Feature 3: Skew Join Optimization
# =============================================
print("\n" + "=" * 60)
print("FEATURE 3: Skew Join Optimization")
print("=" * 60)

spark_aqe.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark_aqe.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "64MB")

skewed = generate_skewed_table(spark_aqe, num_rows=5_000_000, hot_keys=3, hot_key_ratio=0.7)
other = skewed.groupBy("account_number").agg(F.avg("value").alias("avg_val"))

start = time.time()
result3 = skewed.join(other, "account_number")
result3.write.mode("overwrite").parquet("/tmp/case09_skew_aqe")
print(f"✅ AQE Skew Join: {time.time() - start:.1f}s")

spark_aqe.stop()
```

### AQE Configs We Used in Production

```python
# These are the exact configs from our e& pipeline spark-submit command:
{
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
}
```

### What to Look for on Spark UI

```
SQL Tab → Look for "AQE" labels:
  - "AdaptiveSparkPlan (isFinalPlan=true)" — AQE was active
  - "CustomShuffleReader" — AQE coalesced partitions
  - "BroadcastHashJoin" appearing where you expected SortMergeJoin — AQE switched strategy
```

---

## Case 10 — Persist, Cache & Unpersist Strategy

### The Problem
Without caching, Spark **recomputes** a DataFrame from scratch every time you use it. But caching everything wastes memory and causes excessive GC. The key is caching **strategically** and **unpersisting** when done.

### Real-World Context
Our pipeline used the same intermediate DataFrame 3 times (for metrics, ranking, and output), but never cached it — tripling computation time. After adding strategic persist + unpersist, we saved 25-35% runtime.

### Demo Setup

```python
# case_10_persist_cache.py
from pyspark.sql import functions as F
from pyspark.sql.expressions import Window
from pyspark.storage import StorageLevel
from data_generator import get_spark, generate_large_table
import time

spark = get_spark("Case10_PersistCache")

df = generate_large_table(spark, num_rows=10_000_000)

# =============================================
# ❌ BAD: No caching — recomputes 3 times
# =============================================
start = time.time()

# Expensive intermediate computation
expensive_df = (
    df
    .groupBy("account_number", "category")
    .agg(
        F.sum("download_bytes").alias("total_download"),
        F.count("*").alias("event_count")
    )
    .withColumn("avg_download", F.col("total_download") / F.col("event_count"))
)

# Use 1: metrics
metrics = expensive_df.agg(F.avg("total_download"), F.max("event_count")).collect()

# Use 2: top accounts
top_accounts = expensive_df.orderBy(F.desc("total_download")).limit(100).collect()

# Use 3: write output
expensive_df.write.mode("overwrite").parquet("/tmp/case10_bad")

print(f"❌ No caching: {time.time() - start:.1f}s (computed 3 times!)")

# =============================================
# ✅ GOOD: Strategic persist + unpersist
# =============================================
start = time.time()

expensive_df2 = (
    df
    .groupBy("account_number", "category")
    .agg(
        F.sum("download_bytes").alias("total_download"),
        F.count("*").alias("event_count")
    )
    .withColumn("avg_download", F.col("total_download") / F.col("event_count"))
)

# Persist with MEMORY_AND_DISK_SER (serialized = less memory, disk spillable)
expensive_df2.persist(StorageLevel.MEMORY_AND_DISK_SER)
expensive_df2.count()  # force materialization

# Use 1: metrics (reads from cache)
metrics = expensive_df2.agg(F.avg("total_download"), F.max("event_count")).collect()

# Use 2: top accounts (reads from cache)
top_accounts = expensive_df2.orderBy(F.desc("total_download")).limit(100).collect()

# Use 3: write output (reads from cache)
expensive_df2.write.mode("overwrite").parquet("/tmp/case10_good")

# ⚠️ CRITICAL: Unpersist when done to free memory
expensive_df2.unpersist()

print(f"✅ With caching: {time.time() - start:.1f}s (computed once, read 3 times)")
```

### Storage Levels Cheat Sheet

```
StorageLevel               | Memory | Disk | Serialized | Speed | Memory Cost
MEMORY_ONLY                |   ✅   |  ❌  |    ❌      | ⚡⚡⚡  | High
MEMORY_AND_DISK            |   ✅   |  ✅  |    ❌      | ⚡⚡   | High
MEMORY_AND_DISK_SER  ⭐    |   ✅   |  ✅  |    ✅      | ⚡    | Medium  ← Best for most cases
DISK_ONLY                  |   ❌   |  ✅  |    ✅      | 🐌    | None

Our pipeline used MEMORY_AND_DISK_SER_2 (_2 = replicated for fault tolerance on YARN)
```

### What to Look for on Spark UI

```
Storage Tab:
  - Shows all cached DataFrames, their size, and memory/disk usage
  - After unpersist: should disappear from this tab
  
Stages Tab:
  - Without cache: same stage appears 3 times (recomputation)
  - With cache: stage appears once, subsequent uses show "in-memory table scan"
```

---

## Case 11 — Shuffle Partition Tuning

### The Problem
The default `spark.sql.shuffle.partitions = 200` is rarely optimal. Too few → large partitions (OOM, GC pressure). Too many → tiny partitions (scheduler overhead, small files).

### Demo Setup

```python
# case_11_shuffle_partitions.py
from pyspark.sql import functions as F
from data_generator import get_spark, generate_large_table
import time

results = []
for num_partitions in [10, 50, 200, 500, 1000, 2000]:
    spark = get_spark(f"Case11_Partitions_{num_partitions}", {
        "spark.sql.shuffle.partitions": str(num_partitions),
        "spark.sql.adaptive.enabled": "false"  # disable AQE to see raw effect
    })
    
    df = generate_large_table(spark, num_rows=10_000_000)
    
    start = time.time()
    result = (
        df.groupBy("account_number")
        .agg(
            F.sum("download_bytes").alias("total"),
            F.count("*").alias("cnt")
        )
    )
    result.write.mode("overwrite").parquet(f"/tmp/case11_{num_partitions}")
    elapsed = time.time() - start
    
    results.append((num_partitions, elapsed))
    print(f"Partitions={num_partitions:>5}: {elapsed:.1f}s")
    spark.stop()

# Print summary
print("\n" + "=" * 40)
print(f"{'Partitions':>10} | {'Time (s)':>10}")
print("-" * 40)
for p, t in results:
    bar = "█" * int(t * 2)
    print(f"{p:>10} | {t:>10.1f} | {bar}")
```

### Sizing Formula

```python
# Formula: target partition size = 128MB - 256MB
optimal_partitions = total_shuffle_data_size_bytes / (128 * 1024 * 1024)

# Examples:
#   10GB shuffle data → 10000MB / 128MB ≈ 80 partitions
#   100GB shuffle data → 100000MB / 128MB ≈ 800 partitions
#   Our pipeline (87GB) → 87000MB / 128MB ≈ 680 → we used 540

# Quick check: look at Shuffle Write in the Stages tab
# Divide that by your partition count — each partition should be 64MB-256MB
```

### What to Look for on Spark UI

```
Stages Tab → Summary Metrics:
  
  Too few partitions (10):
    Shuffle Write / Records: huge per task, possible OOM
    GC Time: high (> 10% of duration)
    
  Too many partitions (2000):
    Scheduler Delay: significant (lots of tiny tasks)
    Task Duration: min and max nearly the same but total time is high
    
  Just right (200-500):
    Each task: ~128MB input, 2-10s duration
    GC Time: < 5% of task duration
```

---

## Case 12 — GC Tuning & Memory Management

### The Problem
Spark runs on the JVM. When Garbage Collection (GC) kicks in, **all processing pauses**. In our pipeline, GC was consuming 38 seconds per task out of 1.4 hours — and the default GC algorithm wasn't handling large heaps well.

### Demo Setup

```python
# case_12_gc_tuning.py
# This demo is primarily about spark-submit configuration, not code changes.
# Run the same job with different GC settings and compare.

from data_generator import get_spark, generate_large_table
from pyspark.sql import functions as F
import time

# =============================================
# Test 1: Default GC (Parallel GC)
# =============================================
spark1 = get_spark("Case12_DefaultGC", {
    "spark.executor.memory": "4g",
    "spark.executor.extraJavaOptions": "-XX:+UseParallelGC"
})

df1 = generate_large_table(spark1, num_rows=5_000_000)
start = time.time()
result1 = df1.groupBy("account_number").agg(F.sum("download_bytes")).count()
print(f"Parallel GC: {time.time() - start:.1f}s")
spark1.stop()

# =============================================
# Test 2: G1GC (Recommended for Spark)
# =============================================
spark2 = get_spark("Case12_G1GC", {
    "spark.executor.memory": "4g",
    "spark.executor.extraJavaOptions": 
        "-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseCompressedOops"
})

df2 = generate_large_table(spark2, num_rows=5_000_000)
start = time.time()
result2 = df2.groupBy("account_number").agg(F.sum("download_bytes")).count()
print(f"G1GC:        {time.time() - start:.1f}s")
spark2.stop()
```

### Production spark-submit GC Configs

```bash
# The exact GC flags we used in production (from our e& pipeline):
--conf spark.executor.extraJavaOptions='-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseCompressedOops'
--conf spark.driver.extraJavaOptions='-XX:+UseG1GC -XX:MaxGCPauseMillis=200'
```

### Memory Layout Explained

```
Executor Memory (e.g., 20g):
├── Execution Memory (60%) ← shuffles, joins, sorts, aggregations
├── Storage Memory (40%)   ← cached DataFrames, broadcast variables
│   (Unified Memory: these two can borrow from each other)
├── User Memory            ← UDFs, internal metadata
└── Reserved Memory (300MB)← Spark internal overhead

Overhead Memory (spark.executor.memoryOverhead):
└── Off-heap, container memory, network buffers
    Default: max(384MB, 0.10 * executorMemory)
    Set higher for broadcast-heavy jobs

Our production config:
  --executor-memory 20g        ← JVM heap
  --conf spark.executor.memoryOverhead=4g  ← Off-heap
  Total per executor container: 24g
```

### What to Look for on Spark UI

```
Executors Tab:
  - GC Time column: should be < 5% of total task time
  - If GC Time > 10%: increase executor memory or reduce data per partition

Stages Tab → Task Metrics:
  - Sort by GC Time descending
  - Compare before/after switching to G1GC

Environment Tab:
  - Verify your JVM flags actually took effect
  - Search for "java.opts" or "G1GC"
```

---

## Bonus — The spark-submit Cheat Sheet

### The Command That Went from 16 Hours to 2 Hours

```bash
# BEFORE (original — 16 hours, under-resourced):
spark-submit \
  --master yarn \
  --num-executors 30 \
  --executor-memory 30g \
  --executor-cores 12 \
  --driver-memory 25g \
  app.jar

# AFTER (optimized — 2 hours):
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name EDM_CUSTOMERS_SEGMENTATIONS_OPTIMIZED \
  \
  # === RESOURCES === #
  --num-executors 45 \
  --executor-cores 4 \
  --executor-memory 20g \
  --driver-memory 16g \
  --driver-maxResultSize 8g \
  \
  # === AQE === #
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.sql.adaptive.skewJoin.enabled=true \
  --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128MB \
  \
  # === SHUFFLING === #
  --conf spark.sql.shuffle.partitions=400 \
  --conf spark.default.parallelism=180 \
  \
  # === SERIALIZATION === #
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  \
  # === GC === #
  --conf spark.executor.extraJavaOptions='-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseCompressedOops' \
  --conf spark.driver.extraJavaOptions='-XX:+UseG1GC -XX:MaxGCPauseMillis=200' \
  \
  # === TIMEOUTS === #
  --conf spark.network.timeout=800s \
  --conf spark.executor.heartbeatInterval=60s \
  --conf spark.sql.broadcastTimeout=3600 \
  --conf spark.rpc.askTimeout=600s \
  \
  # === SPECULATION === #
  --conf spark.speculation=true \
  --conf spark.speculation.multiplier=1.5 \
  \
  # === DYNAMIC ALLOCATION === #
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=20 \
  --conf spark.dynamicAllocation.maxExecutors=100 \
  --conf spark.dynamicAllocation.initialExecutors=40 \
  \
  app.jar args
```

### Why Each Change Mattered

| Change | Before | After | Why |
|--------|--------|-------|-----|
| executor-cores | 12 | 4 | 12 cores/executor causes memory contention and HDFS throttling |
| executor-memory | 30g | 20g | Smaller heaps = faster GC, more executors possible |
| num-executors | 30 | 45 | More parallelism (45×4=180 cores vs 30×12=360 but contended) |
| AQE | off | on | Auto-coalesces partitions, handles skew at runtime |
| shuffle.partitions | default 200 | 400 | Matched to 87GB data / 128MB target per partition |
| G1GC | ParallelGC | G1GC | Better pause times for large heaps |
| Speculation | off | on | Automatically re-launches slow tasks |
| KryoSerializer | JavaSerializer | Kryo | 10x faster serialization |

---

## 🎬 Demo Recording Tips

### For Each Case

1. **Show the problem first** — run the "BAD" version, open Spark UI, point at the bottleneck.
2. **Explain WHY it's slow** — point at the specific metric (shuffle size, task skew, GC time).
3. **Apply the fix** — run the "GOOD" version.
4. **Show the result** — side-by-side Spark UI comparison.
5. **Show the numbers** — before/after timing and the percentage improvement.

### Suggested Demo Order (YouTube Series)

```
Episode 1: Spark UI Deep Dive (How to Monitor)
Episode 2: Broadcast Join vs Shuffle Join (Case 01)
Episode 3: Bucketing (Case 02) + Partitioning (Case 03)
Episode 4: Data Skew — Detection, Salting, AQE (Cases 04, 06, 09)
Episode 5: Coalesce vs Repartition + Shuffle Tuning (Cases 05, 11)
Episode 6: Catalyst Optimizer — Reading Explain Plans (Case 07)
Episode 7: Compression, Caching, GC Tuning (Cases 08, 10, 12)
Episode 8: The Full spark-submit — Putting It All Together (Bonus)
```

---

## 📚 References

- [Spark Official Tuning Guide](https://spark.apache.org/docs/latest/tuning.html)
- [Adaptive Query Execution](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution)
- [Understanding Spark UI](https://spark.apache.org/docs/latest/web-ui.html)

---

> **Built from real production experience** — optimizing an 87GB/day  ETL pipeline on Cloudera CDP with YARN, processing customer behavioral segmentation across 2.7M+ records per batch.
