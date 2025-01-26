import logging

from pyspark.sql import SparkSession, functions as f

from src.schemas import RATING_SCHEMA, MOVIE_SCHEMA


logger = logging.getLogger(__name__)


def main():
    logger.info("Initialize SparkSession")
    spark = SparkSession.builder \
        .appName("AnalyzeMoviesDataDFAPI") \
        .master("spark://spark:7077") \
        .getOrCreate()

    logger.info("Loading DataFrames...")

    ratings_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferschema", "true") \
        .load("./data/ratings.csv")

    logger.info("Ratings DataFrame loaded!")

    movies_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferschema", "true") \
        .load("./data/movies.csv") \
        .alias("movies_df")

    logger.info("Movies DataFrame loaded!")
    logger.info("DataFrames loaded!")

    logger.info("Starting transformations!")

    ratings_with_year_and_counts_df = ratings_df.select("movieId", "timestamp") \
        .withColumn('timestamp', f.from_unixtime(f.col('timestamp'))) \
        .withColumn('year', f.year(f.col('timestamp'))) \
        .drop("timestamp") \
        .groupBy("year", "movieId").agg(f.count(f.col("movieId")).alias("count")) \
        .alias("ratings_with_year_and_counts_df")

    logger.info("Calculated rating counts by movie and year.")

    ratings_max_counts_by_year_df = ratings_with_year_and_counts_df.groupBy("year").agg(f.max(f.col("count")).alias("count"))

    ratings_df = ratings_with_year_and_counts_df.join(
            ratings_max_counts_by_year_df.alias("max_df"),
            (ratings_with_year_and_counts_df["count"] == ratings_max_counts_by_year_df["count"])
            &
            (ratings_with_year_and_counts_df["year"] == ratings_max_counts_by_year_df["year"]),
            "left",
        ) \
        .filter(
            ratings_with_year_and_counts_df["count"] == ratings_max_counts_by_year_df["count"]
        ) \
        .select(
            ratings_with_year_and_counts_df.movieId.alias("movieId"),
            f.col("ratings_with_year_and_counts_df.year"),
            f.col('ratings_with_year_and_counts_df.count'),
        ).alias("ratings_df")

    logger.info("Filtered max counts by year")

    result_df = movies_df.join(
            ratings_df,
            'movieId',
            'inner'
        ) \
        .select(
            movies_df.title.alias("Title"),
            ratings_df.year.alias("Year"),
            f.col("ratings_df.count").alias("Total Rates"),
        ).orderBy("year")

    logger.info("Finished transformations!")

    result_df.show(100, truncate=False)

    logger.info("Results shown")

    spark.stop()

    logger.info("SparkSession stopped")


if __name__ == "__main__":
    main()