import os
import re
from datetime import datetime

from pyspark.sql import SparkSession, functions as F, types as T

# -------------------------------
# Mode detection (S3 vs local)
# -------------------------------
BUCKET = os.environ.get("SPARK_LOGS_BUCKET")  # e.g. s3://yc1317-assignment-spark-cluster-logs
USE_S3 = bool(BUCKET)

if USE_S3:
    INPUT_GLOB  = f"{BUCKET}/data/application_*/container_*.log"
    OUTPUT_BASE = f"{BUCKET}/output/problem1"
else:
    # local/sample mode
    INPUT_GLOB  = "data/sample/application_*/container_*.log"
    OUTPUT_BASE = "data/output/problem1"

print("✅ Running in {} mode".format("S3" if USE_S3 else "LOCAL"))
print("  Input: ", INPUT_GLOB)
print("  Output:", OUTPUT_BASE)

# -------------------------------
# Spark session
# -------------------------------
spark = (
    SparkSession.builder
    .appName("Problem1_LogLevelDistribution")
    # Keep safe output commit algorithm, but remove deprecated PathOutputCommitProtocol
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config("spark.sql.parquet.output.committer.class", "org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter")
    .config("spark.hadoop.fs.s3a.committer.name", "directory")
    .config("spark.hadoop.fs.s3a.committer.magic.enabled", "false")
    .getOrCreate()
)

# -------------------------------
# Read raw lines
# -------------------------------
df = spark.read.text(INPUT_GLOB).withColumnRenamed("value", "raw")

# Only these four levels are needed per assignment
LEVELS = ["INFO", "WARN", "ERROR", "DEBUG"]
LEVELS_REGEX = r"\b(INFO|WARN|ERROR|DEBUG)\b"

df_levels = (
    df.withColumn("log_level", F.regexp_extract(F.col("raw"), LEVELS_REGEX, 1))
      .filter(F.col("log_level") != "")
)

# -------------------------------
# Aggregations
# -------------------------------
counts_df = (
    df_levels.groupBy("log_level")
             .agg(F.count(F.lit(1)).alias("count"))
)

# Fix ordering by the required level order (INFO,WARN,ERROR,DEBUG) if present
order_expr = F.when(F.col("log_level")=="INFO", 1) \
              .when(F.col("log_level")=="WARN", 2) \
              .when(F.col("log_level")=="ERROR",3) \
              .when(F.col("log_level")=="DEBUG",4) \
              .otherwise(99)

counts_df = counts_df.orderBy(order_expr)

# 10-line random sample with quotes around log_entry
sample_df = (
    df_levels.select(
        F.col("raw").alias("log_entry"),
        F.col("log_level")
    )
    .orderBy(F.rand())
    .limit(10)
)

# Totals and summary
total_lines = df.count()
total_with_levels = df_levels.count()
unique_levels = counts_df.count()

# Collect distribution for summary percentages
counts = {row["log_level"]: row["count"] for row in counts_df.collect()}
def pct(v): 
    return 0.0 if total_with_levels == 0 else (100.0 * v / total_with_levels)

# -------------------------------
# Helpers: write single files with exact names
# (coalesce(1) -> temp dir -> rename "part-*" to final filename)
# -------------------------------
def _rename_single_csv(temp_dir: str, final_path: str):
    """
    Use Hadoop FS via py4j to rename the single output part file to the requested name.
    Works for both local FS and S3 (s3a).
    """
    jvm = spark._jvm
    sc = spark.sparkContext
    conf = sc._jsc.hadoopConfiguration()
    Path = jvm.org.apache.hadoop.fs.Path
    FileSystem = jvm.org.apache.hadoop.fs.FileSystem

    fs = FileSystem.get(Path(temp_dir).toUri(), conf)
    status = fs.listStatus(Path(temp_dir))
    part = None
    for s in status:
        name = s.getPath().getName()
        if name.startswith("part-") and name.endswith(".csv"):
            part = s.getPath()
            break
    if part is None:
        raise RuntimeError(f"No part-*.csv found under {temp_dir}")
    # Make sure parent exists
    target = Path(final_path)
    parent = target.getParent()
    if not fs.exists(parent):
        fs.mkdirs(parent)
    # If target exists, remove it
    if fs.exists(target):
        fs.delete(target, False)
    fs.rename(part, target)
    # cleanup temp dir
    fs.delete(Path(temp_dir), True)

def _rename_single_txt(temp_dir: str, final_path: str):
    jvm = spark._jvm
    sc = spark.sparkContext
    conf = sc._jsc.hadoopConfiguration()
    Path = jvm.org.apache.hadoop.fs.Path
    FileSystem = jvm.org.apache.hadoop.fs.FileSystem

    fs = FileSystem.get(Path(temp_dir).toUri(), conf)
    status = fs.listStatus(Path(temp_dir))
    part = None
    for s in status:
        name = s.getPath().getName()
        # Spark "text" format writes "part-*" with no extension
        if name.startswith("part-"):
            part = s.getPath()
            break
    if part is None:
        raise RuntimeError(f"No part-* file found under {temp_dir}")
    target = Path(final_path)
    parent = target.getParent()
    if not fs.exists(parent):
        fs.mkdirs(parent)
    if fs.exists(target):
        fs.delete(target, False)
    fs.rename(part, target)
    fs.delete(Path(temp_dir), True)

# -------------------------------
# Write outputs
# -------------------------------
COUNTS_CSV = f"{OUTPUT_BASE}/problem1_counts.csv"
SAMPLE_CSV = f"{OUTPUT_BASE}/problem1_sample.csv"
SUMMARY_TXT = f"{OUTPUT_BASE}/problem1_summary.txt"

# 1) counts csv
temp_counts = f"{OUTPUT_BASE}/_counts_tmp_{int(datetime.utcnow().timestamp())}"
(counts_df
 .select("log_level", "count")
 .coalesce(1)
 .write.mode("overwrite")
 .option("header", True)
 .csv(temp_counts))
_rename_single_csv(temp_counts, COUNTS_CSV)

# 2) sample csv (quoteAll to match expected style)
temp_sample = f"{OUTPUT_BASE}/_sample_tmp_{int(datetime.utcnow().timestamp())}"
(sample_df
 .coalesce(1)
 .write.mode("overwrite")
 .option("header", True)
 .option("quoteAll", True)
 .csv(temp_sample))
_rename_single_csv(temp_sample, SAMPLE_CSV)

# 3) summary txt
lines = []
lines.append(f"Total log lines processed: {total_lines}")
lines.append(f"Total lines with log levels: {total_with_levels}")
lines.append(f"Unique log levels found: {unique_levels}")
lines.append("")
lines.append("Log level distribution:")

for lvl in LEVELS:
    if lvl in counts:
        lines.append(f"  {lvl:<5}: {counts[lvl]:>10,} ({pct(counts[lvl]):6.2f}%)")

summary_str = "\n".join(lines)

# Save as text (single file)
temp_summary = f"{OUTPUT_BASE}/_summary_tmp_{int(datetime.utcnow().timestamp())}"
spark.sparkContext.parallelize([summary_str], 1).saveAsTextFile(temp_summary)
_rename_single_txt(temp_summary, SUMMARY_TXT)

print("✅ Done. Wrote:")
print(" -", COUNTS_CSV)
print(" -", SAMPLE_CSV)
print(" -", SUMMARY_TXT)

spark.stop()