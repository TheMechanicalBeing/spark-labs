## Objective
Analyse the __MovieLens Dataset__ to answer the following questions using Apache Spark. You must implement the solutions using Spark DataFrame API, Spark SQL, and RDD Transformations/Reductions and Python.

### Questions

1. Movie Popularity by Year
- Using the timestamp column from the Ratings dataset, identify the most-rated movie for each year.
2. Top Genres by Average Rating
- Determine which genres have the highest average rating. Show the top 5 genres based on average ratings.
3. Top-Rated Movies by Genre
- For each genre, identify the top 5 movies based on their average ratings (minimum of 10 ratings per movie).

You will use the following datasets:
- Movies: movieId, title, genres
- Ratings: userId, movieId, rating, timestamp