from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("LoadRecommendationsToPostgres") \
    .getOrCreate()

recommendations = spark.read.parquet(
    "hdfs://namenode:8020/data/hm/gold/recommendations"
)

jdbc_url = "jdbc:postgresql://postgres:5432/hm_recommendations"

properties = {
    "user": "hm_user",
    "password": "hm_password",
    "driver": "org.postgresql.Driver"
}

recommendations.write \
    .mode("overwrite") \
    .jdbc(
        url=jdbc_url,
        table="recommendations",
        properties=properties
    )

print("=== RECOMMENDATIONS LOADED TO POSTGRES ===")

spark.stop()