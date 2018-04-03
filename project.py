from pyspark.sql import SparkSession

if __name__ == "__main__":

        spark = SparkSession.builder.appName("Movielens").config("spark.cassandra.connection.host","127.0.0.1").getOrCreate()
        #table movies
        readMovies = spark.read\
                .format("org.apache.spark.sql.cassandra")\
                .options(table = "movies",keyspace = "movielens")\
                .load()
        readMovies.createOrReplaceTempView("movies")
        #table ratings
        readRatings = spark.read\
                .format("org.apache.spark.sql.cassandra")\
                .options(table = "ratings",keyspace = "movielens")\
                .load()
        readRatings.createOrReplaceTempView("ratings")
        #table tags
        readTags = spark.read\
                .format("org.apache.spark.sql.cassandra")\
                .options(table = "tags",keyspace = "movielens")\
                .load()
        readTags.createOrReplaceTempView("tags")
      
		#Thong ke so luong phim theo tung the loai
		sqlDFb = spark.sql("Select genres, COUNT(movie_id) as total from movies GROUP BY genres")
		sqlDFb.show()
		#  Tim chat luong phim cua tung the loai
		sqlDFb = spark.sql("Select AVG(rating) as avg_rating, genres from ratings, 
		movies where movies.movie_id = ratings.movie_id group by genres order by AVG(rating) desc")
		sqlDFb.show()
		spark.stop()

