import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def clean_data(df):
    filtered_df = df.filter((col("amount") > 0) & col("name").isNotNull())
    final_df = filtered_df.withColumn("amount_with_tax", col("amount") * 1.20)
    return final_df

def main(hdfs_path):
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.parquet(hdfs_path)
    cleaned = clean_data(df)
    cleaned.show()
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        main(sys.argv[1])