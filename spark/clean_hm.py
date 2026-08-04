from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, to_date

spark = SparkSession.builder \
    .appName("HM-Cleaning") \
    .getOrCreate()

raw_path = "hdfs://namenode:8020/data/hm/raw"
silver_path = "hdfs://namenode:8020/data/hm/silver"

print("=== READING DATA ===")

customers = spark.read \
    .option("header", False) \
    .csv(f"{raw_path}/customers.csv") \
    .toDF(
        "customer_id",
        "FN",
        "Active",
        "club_member_status",
        "fashion_news_frequency",
        "age",
        "postal_code"
    )

articles = spark.read \
    .option("header", False) \
    .csv(f"{raw_path}/articles.csv") \
    .toDF(
        "article_id",
        "product_code",
        "prod_name",
        "product_type_no",
        "product_type_name",
        "product_group_name",
        "graphical_appearance_no",
        "graphical_appearance_name",
        "colour_group_code",
        "colour_group_name",
        "perceived_colour_value_id",
        "perceived_colour_value_name",
        "perceived_colour_master_id",
        "perceived_colour_master_name",
        "department_no",
        "department_name",
        "index_code",
        "index_name",
        "index_group_no",
        "index_group_name",
        "section_no",
        "section_name",
        "garment_group_no",
        "garment_group_name",
        "detail_desc"
    )

transactions = spark.read \
    .option("header", False) \
    .csv(f"{raw_path}/transactions_train.csv") \
    .toDF(
        "t_dat",
        "customer_id",
        "article_id",
        "price",
        "sales_channel_id"
    )

print("=== READING DONE ===")

print("=== CLEANING CUSTOMERS ===")

customers = customers \
    .withColumn("customer_id", trim(col("customer_id"))) \
    .withColumn("club_member_status", lower(trim(col("club_member_status")))) \
    .withColumn("fashion_news_frequency", lower(trim(col("fashion_news_frequency")))) \
    .withColumn("age", col("age").cast("double")) \
    .filter(col("customer_id").isNotNull()) \
    .dropDuplicates(["customer_id"])

print("=== CUSTOMERS CLEANED ===")

print("=== CLEANING ARTICLES ===")

articles = articles \
    .withColumn("article_id", trim(col("article_id"))) \
    .withColumn("product_code", trim(col("product_code"))) \
    .withColumn("prod_name", trim(col("prod_name"))) \
    .withColumn("product_type_no", col("product_type_no").cast("int")) \
    .withColumn("product_type_name", trim(col("product_type_name"))) \
    .withColumn("product_group_name", trim(col("product_group_name"))) \
    .withColumn("colour_group_name", trim(col("colour_group_name"))) \
    .withColumn("department_name", trim(col("department_name"))) \
    .withColumn("index_code", trim(col("index_code"))) \
    .withColumn("index_name", trim(col("index_name"))) \
    .withColumn("section_name", trim(col("section_name"))) \
    .withColumn("garment_group_name", trim(col("garment_group_name"))) \
    .filter(col("article_id").isNotNull()) \
    .dropDuplicates(["article_id"])

print("=== ARTICLES CLEANED ===")

print("=== CLEANING TRANSACTIONS ===")

transactions = transactions \
    .withColumn("t_dat", to_date(col("t_dat"))) \
    .withColumn("customer_id", trim(col("customer_id"))) \
    .withColumn("article_id", trim(col("article_id"))) \
    .withColumn("price", col("price").cast("double")) \
    .withColumn("sales_channel_id", col("sales_channel_id").cast("int")) \
    .filter(
        col("t_dat").isNotNull() &
        col("customer_id").isNotNull() &
        col("article_id").isNotNull() &
        col("price").isNotNull() &
        (col("price") > 0)
    ) \
    .dropDuplicates([
        "t_dat",
        "customer_id",
        "article_id",
        "price",
        "sales_channel_id"
    ])

print("=== TRANSACTIONS CLEANED ===")

print("=== WRITING SILVER ===")

customers.write \
    .mode("overwrite") \
    .parquet(f"{silver_path}/customers")

print("Customers written")

articles.write \
    .mode("overwrite") \
    .parquet(f"{silver_path}/articles")

print("Articles written")

transactions.write \
    .mode("overwrite") \
    .parquet(f"{silver_path}/transactions")

print("Transactions written")

print("=== SILVER DATA ===")

print("Customers:", customers.count())
print("Articles:", articles.count())
print("Transactions:", transactions.count())

print("=== CLEANING COMPLETE ===")

spark.stop()
