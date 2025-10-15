# Importing Spark libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg


# Creating Spark session
spark = SparkSession.builder \
    .appName("Wine Distributed Feature Analysis") \
    .getOrCreate()


# Loading dataset
df = spark.read.csv(
    "data/generated_big_wine_dataset.csv",
    header=True,
    inferSchema=True
)


# Extracting feature columns
feature_columns = df.columns[1:]


# Performing distributed aggregation
aggregated_results = df.groupBy("wine_type").agg(
    *[avg(col(c)).alias(f"avg_{c}") for c in feature_columns]
)


# Showing results
aggregated_results.show(truncate=False)


# Saving output
aggregated_results.write.mode("overwrite").csv(
    "output/spark_feature_analysis"
)

# Stopping Spark session
spark.stop()
