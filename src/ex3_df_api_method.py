import logging

from pyspark.sql import SparkSession, functions as f
from pyspark.sql.window import Window

from src.schemas import MOVIE_SCHEMA, RATING_SCHEMA


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
        .load("./data/ratings.csv") \
        .alias("ratings_df")

    logger.info("Ratings DataFrame loaded!")

    movies_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferschema", "true") \
        .load("./data/movies.csv") \
        .alias("movies_df")

    logger.info("Movies DataFrame loaded!")
    logger.info("DataFrames loaded!")

    logger.info("Starting transformations!")

    movies_df = movies_df.withColumn("genres", f.split("genres", "\|")) \
        .withColumn("genres", f.explode("genres"))

    logger.info("Exploded genres in movies_df")

    ratings_more_than_10_df = ratings_df.groupBy("movieId").agg(f.count("movieId").alias("count")) \
        .filter(f.col("count") > 10)

    movie_with_rating_df = movies_df.join(
            ratings_more_than_10_df,
            "movieId",
            "inner"
        ) \
        .join(
            ratings_df,
            "movieId",
            "inner"
        ) \
        .select(
            movies_df.movieId,
            movies_df.title,
            movies_df.genres,
            f.col("ratings_df.rating").alias("rating"),
        )

    logger.info("Filtered movies and joined with ratings_df")

    window_spec = Window.partitionBy("genres").orderBy(f.col("average_rating").desc())

    result_df = movie_with_rating_df \
        .groupBy("genres", "movieId", "title").agg(f.avg("rating").alias("average_rating")) \
        .withColumn("rank", f.rank().over(window_spec)) \
        .filter(f.col("rank") <= 5) \
        .alias("result_df") \
        .select(
            movie_with_rating_df.genres.alias("Genre"),
            movie_with_rating_df.title.alias("Movie"),
            f.col("result_df.average_rating").alias("AverageRating"),
            f.col("result_df.rank").alias("Ranking"),
        )

    logger.info("Finished transformations!")

    result_df.show(100, truncate=False)

    logger.info("Results shown")

    spark.stop()

    logger.info("SparkSession stopped")


if __name__ == "__main__":
    main()