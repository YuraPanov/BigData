from pyspark.sql import SparkSession
import sys

def main():
    print("Начинаем анализ SWITRS...")

    spark = SparkSession.builder \
        .appName("SWITRS Analysis") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    CSV_PATH = "/opt/spark/work-dir/collisions_small.csv"

    df = spark.read.csv(CSV_PATH, header=True, inferSchema=True)
    df.createOrReplaceTempView("collisions")

    top10 = spark.sql("""
        SELECT county_location, COUNT(*) AS accidents
        FROM collisions
        GROUP BY county_location
        ORDER BY accidents DESC
        LIMIT 10
    """)

    # top10.show(truncate=False)
    #
    # output_path = sys.argv[1]
    # top10.write.csv(output_path, mode="overwrite", header=True)

    spark.stop()

if __name__ == "__main__":
    main()
