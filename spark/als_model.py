from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, explode
from pyspark.sql.window import Window
from pyspark.ml.recommendation import ALS

spark = SparkSession.builder \
    .appName("HM-ALS-Recommendations") \
    .getOrCreate()

features_path = "hdfs://namenode:8020/data/hm/gold/model_features"
articles_path = "hdfs://namenode:8020/data/hm/silver/articles"
output_path = "hdfs://namenode:8020/data/hm/gold/recommendations"

print("=== READING DATA ===")

features = spark.read.parquet(features_path)
articles = spark.read.parquet(articles_path)

print("=== CREATING ID MAPPINGS ===")

customers = features \
    .select("customer_id") \
    .distinct() \
    .withColumn(
        "user_id",
        row_number().over(
            Window.orderBy("customer_id")
        ).cast("int")
    )

items = features \
    .select("article_id") \
    .distinct() \
    .withColumn(
        "item_id",
        row_number().over(
            Window.orderBy("article_id")
        ).cast("int")
    )

ratings = features \
    .join(customers, "customer_id") \
    .join(items, "article_id") \
    .select(
        "user_id",
        "item_id",
        col("rating").cast("float")
    )

print("=== TRAINING ALS ===")

als = ALS(
    userCol="user_id",
    itemCol="item_id",
    ratingCol="rating",
    implicitPrefs=True,
    rank=20,
    maxIter=10,
    regParam=0.1,
    alpha=1.0,
    coldStartStrategy="drop",
    nonnegative=True,
    seed=42
)

model = als.fit(ratings)

print("=== GENERATING RECOMMENDATIONS ===")

recommendations = model.recommendForAllUsers(20)

recommendations = recommendations \
    .select(
        "user_id",
        explode("recommendations").alias("recommendation")
    )

recommendations = recommendations \
    .select(
        "user_id",
        col("recommendation.item_id").alias("item_id"),
        col("recommendation.rating").alias("prediction")
    )

print("=== MAPPING IDS ===")

recommendations = recommendations \
    .join(
        customers,
        "user_id"
    ) \
    .join(
        items,
        "item_id"
    ) \
    .select(
        "customer_id",
        "article_id",
        "prediction"
    )

print("=== REMOVING PURCHASED PRODUCTS ===")

purchased = features \
    .select(
        "customer_id",
        "article_id"
    ) \
    .distinct()

recommendations = recommendations \
    .join(
        purchased.withColumn("already_bought", col("article_id")),
        (
            (recommendations.customer_id == purchased.customer_id) &
            (recommendations.article_id == purchased.article_id)
        ),
        "left_anti"
    )

print("=== ADDING PRODUCT INFORMATION ===")

article_info = articles.select(
    "article_id",
    "prod_name",
    "product_type_name",
    "product_group_name",
    "department_name",
    "section_name",
    "index_name"
)

recommendations = recommendations \
    .join(
        article_info,
        "article_id",
        "left"
    )

print("=== SELECTING TOP 10 ===")

window = Window \
    .partitionBy("customer_id") \
    .orderBy(col("prediction").desc())

recommendations = recommendations \
    .withColumn(
        "recommendation_rank",
        row_number().over(window)
    ) \
    .filter(
        col("recommendation_rank") <= 10
    )

print("=== WRITING GOLD RECOMMENDATIONS ===")

recommendations \
    .select(
        "customer_id",
        "article_id",
        "prediction",
        "recommendation_rank",
        "prod_name",
        "product_type_name",
        "product_group_name",
        "department_name",
        "section_name",
        "index_name"
    ) \
    .write \
    .mode("overwrite") \
    .parquet(output_path)

print("=== ALS RECOMMENDATIONS WRITTEN ===")
print("=== ALS MODEL COMPLETE ===")

spark.stop()
