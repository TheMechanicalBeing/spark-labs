from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    ArrayType,
    DoubleType, IntegerType,
)

MOVIE_SCHEMA = StructType([
    StructField('movieId', LongType()),
    StructField('title', StringType()),
    StructField('genres', ArrayType(StringType())),
])

LINK_SCHEMA = StructType([
    StructField('movieId', LongType()),
    StructField('imdbId', LongType()),
    StructField('tmdbId', LongType()),
])

RATING_SCHEMA = StructType([
    StructField('UserId', LongType()),
    StructField('movieId', LongType()),
    StructField('rating', IntegerType()),
    StructField('timestamp', LongType()),
])

TAG_SCHEMA = StructType([
    StructField('userId', LongType()),
    StructField('movieId', LongType()),
    StructField('tag', StringType()),
    StructField('timestamp', LongType()),
])