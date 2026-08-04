from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col

spark = SparkSession.builder \
    .appName("HM-Feature-Engineering") \
    .getOrCreate()

processed_path = "hdfs://namenode:8020/data/hm/processed"
gold_path = "hdfs://namenode:8020/data/hm/gold"

print("=== READING PROCESSED DATA ===")

df = spark.read.parquet(processed_path)

print("=== PROCESSED DATA LOADED ===")
print("Rows:", df.count())

print("=== BUILDING MODEL FEATURES ===")

model_features = df \
    .select("customer_id", "article_id") \
    .groupBy("customer_id", "article_id") \
    .agg(count("*").alias("rating")) \
    .filter(
        col("customer_id").isNotNull() &
        col("article_id").isNotNull()
    )

print("=== MODEL FEATURES CREATED ===")
print("Rows:", model_features.count())

print("=== MODEL FEATURES SAMPLE ===")
model_features.show(10, truncate=False)

print("=== WRITING GOLD MODEL FEATURES ===")

model_features.write \
    .mode("overwrite") \
    .parquet(f"{gold_path}/model_features")

print("=== GOLD MODEL FEATURES WRITTEN ===")
print("=== FEATURE ENGINEERING COMPLETE ===")

spark.stop()
