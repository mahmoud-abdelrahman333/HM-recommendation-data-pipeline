from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder \
    .appName("HM-DataLake") \
    .getOrCreate()

base_path = "hdfs://namenode:8020/data/hm/raw"

customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("FN", DoubleType(), True),
    StructField("Active", DoubleType(), True),
    StructField("club_member_status", StringType(), True),
    StructField("fashion_news_frequency", StringType(), True),
    StructField("age", DoubleType(), True),
    StructField("postal_code", StringType(), True)
])

articles_schema = StructType([
    StructField("article_id", StringType(), True),
    StructField("product_code", StringType(), True),
    StructField("prod_name", StringType(), True),
    StructField("product_type_no", IntegerType(), True),
    StructField("product_type_name", StringType(), True),
    StructField("product_group_name", StringType(), True),
    StructField("graphical_appearance_no", IntegerType(), True),
    StructField("graphical_appearance_name", StringType(), True),
    StructField("colour_group_code", IntegerType(), True),
    StructField("colour_group_name", StringType(), True),
    StructField("perceived_colour_value_id", IntegerType(), True),
    StructField("perceived_colour_value_name", StringType(), True),
    StructField("perceived_colour_master_id", IntegerType(), True),
    StructField("perceived_colour_master_name", StringType(), True),
    StructField("department_no", IntegerType(), True),
    StructField("department_name", StringType(), True),
    StructField("index_code", StringType(), True),
    StructField("index_name", StringType(), True),
    StructField("index_group_no", IntegerType(), True),
    StructField("index_group_name", StringType(), True),
    StructField("section_no", IntegerType(), True),
    StructField("section_name", StringType(), True),
    StructField("garment_group_no", IntegerType(), True),
    StructField("garment_group_name", StringType(), True),
    StructField("detail_desc", StringType(), True)
])

transactions_schema = StructType([
    StructField("t_dat", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("article_id", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("sales_channel_id", IntegerType(), True)
])

customers = spark.read \
    .option("header", True) \
    .schema(customers_schema) \
    .csv(f"{base_path}/customers.csv")

articles = spark.read \
    .option("header", True) \
    .schema(articles_schema) \
    .csv(f"{base_path}/articles.csv")

transactions = spark.read \
    .option("header", False) \
    .schema(transactions_schema) \
    .csv(f"{base_path}/transactions_train.csv")

print("=== CUSTOMERS ===")
customers.printSchema()
print("Rows:", customers.count())
customers.show(5, truncate=False)

print("=== ARTICLES ===")
articles.printSchema()
print("Rows:", articles.count())
articles.show(5, truncate=False)

print("=== TRANSACTIONS ===")
transactions.printSchema()
print("Rows:", transactions.count())
transactions.show(5, truncate=False)

spark.stop()