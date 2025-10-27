#!/usr/bin/env python3
"""
Problem 2: Cluster Usage Analysis
---------------------------------
Analyze Spark log data to extract cluster and application usage statistics.
Generates:
 - problem2_timeline.csv
 - problem2_cluster_summary.csv
 - problem2_stats.txt
 - problem2_bar_chart.png
 - problem2_density_plot.png
"""

import os
import glob
import sys
import fsspec
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# ============================================================
# 1. Setup Spark Session
# ============================================================

spark = (
    SparkSession.builder.appName("Problem2_ClusterUsage")
    .config("spark.executor.memory", "2g")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()
)

# ============================================================
# 2. Determine Input / Output Paths
# ============================================================

USE_S3 = bool(os.environ.get("SPARK_LOGS_BUCKET"))
if USE_S3:
    INPUT_GLOB = f"s3://{os.environ['SPARK_LOGS_BUCKET']}/data/application_*/container_*.log"
    OUTPUT_DIR = f"s3://{os.environ['SPARK_LOGS_BUCKET']}/output/problem2"
    input_paths = [INPUT_GLOB]
else:
    input_paths = glob.glob("data/sample/application_*/*.log")
    OUTPUT_DIR = "data/output/problem2"

print(f"✅ Running in {'S3' if USE_S3 else 'LOCAL'} mode")
print(f"  Output: {OUTPUT_DIR}")

if not input_paths:
    raise FileNotFoundError("❌ No log files found! Check data/sample/ path structure.")
print(f"  Found {len(input_paths)} log files")
print(f"  Example file: {input_paths[0]}")

# ============================================================
# 3. Read Logs into Spark DataFrame
# ============================================================

df_raw = spark.read.text(input_paths).withColumnRenamed("value", "raw")
df_raw = df_raw.withColumn("file_path", F.input_file_name())

# Extract IDs from path
df_raw = df_raw.withColumn("cluster_id", F.regexp_extract(F.col("file_path"), r"application_(\d+)", 1))
df_raw = df_raw.withColumn("application_id", F.regexp_extract(F.col("file_path"), r"(application_\d+_\d+)", 1))

# Extract timestamps
df_raw = df_raw.withColumn(
    "timestamp_str",
    F.regexp_extract(F.col("raw"), r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1)
)
df_raw = df_raw.withColumn(
    "timestamp",
    F.try_to_timestamp(F.col("timestamp_str"), F.lit("yy/MM/dd HH:mm:ss"))
)

df_valid = df_raw.filter(F.col("timestamp").isNotNull())

print("🧪 Sample parsed timestamps:")
for row in df_valid.select("timestamp_str", "timestamp").limit(5).collect():
    print(row)

# ============================================================
# 4. Application-level Aggregation
# ============================================================

apps = (
    df_valid.groupBy("cluster_id", "application_id")
    .agg(
        F.min("timestamp").alias("start_time"),
        F.max("timestamp").alias("end_time"),
    )
    .orderBy("cluster_id", "start_time")
)

apps = apps.withColumn(
    "app_number",
    F.row_number().over(
        Window.partitionBy("cluster_id").orderBy("start_time")
    )
)

# Write timeline CSV
timeline_path = os.path.join(OUTPUT_DIR, "problem2_timeline.csv")
apps.toPandas().to_csv(timeline_path, index=False)
print(f"✅ Wrote timeline: {timeline_path}")

# ============================================================
# 5. Cluster-level Summary
# ============================================================

cluster_summary = (
    apps.groupBy("cluster_id")
    .agg(
        F.count("application_id").alias("num_applications"),
        F.min("start_time").alias("cluster_first_app"),
        F.max("end_time").alias("cluster_last_app"),
    )
    .orderBy(F.desc("num_applications"))
)

cluster_summary_path = os.path.join(OUTPUT_DIR, "problem2_cluster_summary.csv")
cluster_summary.toPandas().to_csv(cluster_summary_path, index=False)
print(f"✅ Wrote cluster summary: {cluster_summary_path}")

# ============================================================
# 6. Summary Stats (S3-safe)
# ============================================================

stats_pd = cluster_summary.toPandas()
total_clusters = len(stats_pd)
total_apps = int(stats_pd["num_applications"].sum())
avg_apps = round(stats_pd["num_applications"].mean(), 2)

stats_txt = [
    f"Total unique clusters: {total_clusters}",
    f"Total applications: {total_apps}",
    f"Average applications per cluster: {avg_apps}",
    "",
    "Most heavily used clusters:",
]
for _, row in stats_pd.head(5).iterrows():
    stats_txt.append(f"  Cluster {row['cluster_id']}: {row['num_applications']} applications")

stats_path = os.path.join(OUTPUT_DIR, "problem2_stats.txt")
with fsspec.open(stats_path, "w") as f:
    f.write("\n".join(stats_txt))
print(f"✅ Wrote stats: {stats_path}")

# ============================================================
# 7. Visualization (S3-safe)
# ============================================================

pd_apps = apps.toPandas()
pd_summary = cluster_summary.toPandas()
sns.set_theme(style="whitegrid")

# --- Bar Chart ---
plt.figure(figsize=(8, 4))
sns.barplot(x="cluster_id", y="num_applications", data=pd_summary, palette="crest")
plt.title("Applications per Cluster")
plt.xlabel("Cluster ID")
plt.ylabel("Number of Applications")
for i, val in enumerate(pd_summary["num_applications"]):
    plt.text(i, val, f"{val}", ha="center", va="bottom")
plt.tight_layout()
bar_chart_path = os.path.join(OUTPUT_DIR, "problem2_bar_chart.png")
with fsspec.open(bar_chart_path, "wb") as f:
    plt.savefig(f, format="png")
plt.close()
print(f"✅ Wrote bar chart: {bar_chart_path}")

# --- Density Plot (largest cluster) ---
largest_cluster = pd_summary.iloc[0]["cluster_id"]
largest_apps = pd_apps[pd_apps["cluster_id"] == largest_cluster].copy()
largest_apps["duration"] = (
    pd.to_datetime(largest_apps["end_time"]) - pd.to_datetime(largest_apps["start_time"])
).dt.total_seconds()

plt.figure(figsize=(8, 4))
sns.histplot(largest_apps["duration"], bins=30, kde=True, log_scale=True)
plt.title(f"Job Duration Distribution (Cluster {largest_cluster}, n={len(largest_apps)})")
plt.xlabel("Duration (seconds, log scale)")
plt.ylabel("Frequency")
plt.tight_layout()
density_path = os.path.join(OUTPUT_DIR, "problem2_density_plot.png")
with fsspec.open(density_path, "wb") as f:
    plt.savefig(f, format="png")
plt.close()
print(f"✅ Wrote density plot: {density_path}")

# ============================================================
# Done
# ============================================================

print("🎉 Problem 2 completed successfully!")
spark.stop()