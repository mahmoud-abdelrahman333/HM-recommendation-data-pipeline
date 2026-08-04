from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum,
    avg,
    month,
    year,
    date_format,
    desc
)

spark = SparkSession.builder \
    .appName("HM-Analytics") \
    .getOrCreate()

processed_path = "hdfs://namenode:8020/data/hm/processed"
gold_path = "hdfs://namenode:8020/data/hm/gold/analytics"

print("=== READING PROCESSED DATA ===")

df = spark.read.parquet(processed_path)

print("=== DATA LOADED ===")
print("Rows:", df.count())

df = df.withColumn(
    "year",
    year(col("t_dat"))
).withColumn(
    "month",
    month(col("t_dat"))
).withColumn(
    "month_name",
    date_format(col("t_dat"), "yyyy-MM")
)

print("=== SALES BY MONTH ===")

sales_by_month = df.groupBy(
    "month_name"
).agg(
    count("*").alias("transactions"),
    countDistinct("customer_id").alias("unique_customers"),
    sum("price").alias("total_sales")
).orderBy("month_name")

sales_by_month.write \
    .mode("overwrite") \
    .parquet(f"{gold_path}/sales_by_month")

print("=== TOP PRODUCTS ===")

top_products = df.groupBy(
    "article_id"
).agg(
    count("*").alias("purchases"),
    countDistinct("customer_id").alias("unique_customers"),
    sum("price").alias("total_sales")
).orderBy(
    desc("purchases")
).limit(100)

top_products.write \
    .mode("overwrite") \
    .parquet(f"{gold_path}/top_products")

print("=== SALES BY PRODUCT GROUP ===")

sales_by_category = df.groupBy(
    "product_group_name"
).agg(
    count("*").alias("transactions"),
    countDistinct("customer_id").alias("unique_customers"),
    sum("price").alias("total_sales"),
    avg("price").alias("average_price")
).orderBy(
    desc("total_sales")
)

sales_by_category.write \
    .mode("overwrite") \
    .parquet(f"{gold_path}/sales_by_category")

print("=== SALES BY CHANNEL ===")

sales_by_channel = df.groupBy(
    "sales_channel_id"
).agg(
    count("*").alias("transactions"),
    countDistinct("customer_id").alias("unique_customers"),
    sum("price").alias("total_sales")
).orderBy(
    desc("total_sales")
)

sales_by_channel.write \
    .mode("overwrite") \
    .parquet(f"{gold_path}/sales_by_channel")

print("=== CUSTOMER ACTIVITY ===")

customer_activity = df.groupBy(
    "customer_id"
).agg(
    count("*").alias("total_purchases"),
    countDistinct("article_id").alias("unique_products"),
    sum("price").alias("total_spending"),
    avg("price").alias("average_purchase_price")
).orderBy(
    desc("total_spending")
)

customer_activity.write \
    .mode("overwrite") \
    .parquet(f"{gold_path}/customer_activity")

print("=== ANALYTICS COMPLETE ===")

print("Analytics outputs:")
print("sales_by_month")
print("top_products")
print("sales_by_category")
print("sales_by_channel")
print("customer_activity")

spark.stop()
