import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, regexp_replace, split, explode, count


def main():
    if len(sys.argv) != 3:
        print("Usage: spark-submit SparkWordCountPython.py <in> <out>", file=sys.stderr)
        sys.exit(2)

    in_file = sys.argv[1]
    out_file = sys.argv[2]

    spark = SparkSession.builder \
        .appName("SparkWordCountPython") \
        .getOrCreate()

    data = spark.read.text(in_file)

    words = (
        data.select(lower("value").alias("lower_value"))
        .select(regexp_replace("lower_value", "[^a-z]", " ").alias("words"))
        .select(split("words", " ").alias("words"))
        .select(explode("words").alias("word"))
        .where("word != ''")
    )

    word_counts = words.groupBy("word").agg(count("*").alias("count"))

    word_counts.write.mode("overwrite").csv(out_file)


if __name__ == "__main__":
    main()
