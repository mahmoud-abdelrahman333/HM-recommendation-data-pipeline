from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum

spark = SparkSession.builder \
    .appName("HM-Processing") \
    .getOrCreate()

silver_path = "hdfs://namenode:8020/data/hm/silver"
processed_path = "hdfs://namenode:8020/data/hm/processed"

print("=== READING SILVER DATA ===")

customers = spark.read.parquet(f"{silver_path}/customers")
articles = spark.read.parquet(f"{silver_path}/articles")
transactions = spark.read.parquet(f"{silver_path}/transactions")

print("=== SILVER DATA LOADED ===")

print("Customers:", customers.count())
print("Articles:", articles.count())
print("Transactions:", transactions.count())

print("=== STARTING JOINS ===")

processed = transactions \
    .join(
        customers,
        transactions.customer_id == customers.customer_id,
        "left"
    ) \
    .join(
        articles,
        transactions.article_id == articles.article_id,
        "left"
    ) \
    .select(
        transactions.t_dat,
        transactions.customer_id,
        transactions.article_id,
        transactions.price,
        transactions.sales_channel_id,
        customers.FN,
        customers.Active,
        customers.club_member_status,
        customers.fashion_news_frequency,
        customers.age,
        customers.postal_code,
        articles.product_code,
        articles.prod_name,
        articles.product_type_no,
        articles.product_type_name,
        articles.product_group_name,
        articles.graphical_appearance_no,
        articles.graphical_appearance_name,
        articles.colour_group_code,
        articles.colour_group_name,
        articles.perceived_colour_value_id,
        articles.perceived_colour_value_name,
        articles.perceived_colour_master_id,
        articles.perceived_colour_master_name,
        articles.department_no,
        articles.department_name,
        articles.index_code,
        articles.index_name,
        articles.index_group_no,
        articles.index_group_name,
        articles.section_no,
        articles.section_name,
        articles.garment_group_no,
        articles.garment_group_name,
        articles.detail_desc
    )

print("=== JOINS COMPLETE ===")

print("=== PROCESSED SCHEMA ===")
processed.printSchema()

print("=== PROCESSED ROWS ===")
print(processed.count())

print("=== NULL CHECK ===")

processed.select(
    col("customer_id").isNull().alias("customer_id_null"),
    col("article_id").isNull().alias("article_id_null"),
    col("prod_name").isNull().alias("product_null"),
    col("age").isNull().alias("age_null")
).groupBy(
    "customer_id_null",
    "article_id_null",
    "product_null",
    "age_null"
).count().show()

print("=== PROCESSED SAMPLE ===")
processed.show(10, truncate=False)

print("=== WRITING PROCESSED DATA ===")

processed.write \
    .mode("overwrite") \
    .parquet(processed_path)

print("=== PROCESSED DATA WRITTEN ===")

print("=== PROCESSING COMPLETE ===")

spark.stop()
