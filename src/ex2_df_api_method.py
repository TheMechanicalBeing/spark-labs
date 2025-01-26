from pyspark.sql import SparkSession, functions as f
import logging

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

    movies_df = movies_df.withColumn("genres", f.split("genres", "\|")) \
        .withColumn("genres", f.explode("genres"))

    logger.info("Exploded genres in movies_df")

    result_df = movies_df.join(
            ratings_df,
            'movieId',
            'inner'
        ) \
        .select(
            movies_df.genres,
            ratings_df.rating,
        ) \
        .groupby("genres").agg(f.avg(ratings_df.rating).alias("average rating")) \
        .orderBy("average rating", ascending=False) \
        .withColumn("average rating", f.round(f.col("average rating"), 2)) \
        .limit(5)

    logger.info("Finished transformations!")

    result_df.show(truncate=False)

    logger.info("Results shown")

    spark.stop()

    logger.info("SparkSession stopped")


if __name__ == "__main__":
    main()
