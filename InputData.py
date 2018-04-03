# input data
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions


# movies (movie_id int, title text, genres text);
def parseInputMovies(line):
    fields = line.split('::')
    return Row(movie_id=int(fields[0]), title=fields[1], genres=fields[2])


# ratings (user_id int, movie_id int, rating int, times_tamp int);
def parseInputRatings(line):
    fields = line.split('::')
    return Row(user_id=int(fields[0]), movie_id=int(fields[1]), rating=int(fields[2]), times_tamp=int(fields[3]))


# tags user_id int, movie_id int, tag text, times_tamp int);
def parseInputTags(line):
    fields = line.split('::')
    return Row(user_id=int(fields[0]), movie_id=int(fields[1]), tag=fields[2], times_tamp=int(fields[3]))


if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("Movielens").config("spark.cassandra.connection.host",
                                                                "127.0.0.1").getOrCreate()

    # Get the raw data movies
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/movies.dat")
    movies = lines.map(parseInputMovies)
    moviesDataset = spark.createDataFrame(movies)

    # Write it into Cassandra
    moviesDataset.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="movies", keyspace="movielens") \
        .save()
    # ===============================================================#
    # Get the raw data ratings
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ratings.dat")
    ratings = lines.map(parseInputRatings)
    ratingsDataset = spark.createDataFrame(ratings)

    # Write it into Cassandra
    ratingsDataset.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="ratings", keyspace="movielens") \
        .save()
    # ===============================================================#
    # Get the raw data tags
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/tags.dat")
    tags = lines.map(parseInputTags)
    tagsDataset = spark.createDataFrame(tags)

    # Write it into Cassandra
    tagsDataset.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="tags", keyspace="movielens") \
        .save()
