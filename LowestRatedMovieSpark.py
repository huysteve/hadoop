from pyspark import SparkConf, SparkContext

def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames
	
def parseInput(line):
    fields = line.split()
    return (int(fields[1]), (float(fields[2]), 1.0))
	
if __name__ == "__main__":
    # The main script - create our SparkContext
    conf = SparkConf().setAppName("WorstMovies")
    sc = SparkContext(conf = conf)	
	
    # Load up our movie ID -> movie name lookup table
    movieNames = loadMovieNames()	
	
    # Load up the raw u.data file
    lines = sc.textFile("hdfs:///user/maria_dev/ml-100k/u.data")
	
    # Convert to (movieID, (rating, 1.0))
    movieRatings = lines.map(parseInput)	
	
    # Reduce to (movieID, (sumOfRatings, totalRatings))
    ratingTotalsAndCount = movieRatings.reduceByKey(lambda x1, x2: (x1[0] + x2[0], x1[1] + x2[1]))
	
    # Map to (rating, averageRating)
    averageRatings = ratingTotalsAndCount.mapValues(lambda y : y[0] / y[1])	
	
    # Sort by average rating
    sortedMovies = averageRatings.sortBy(lambda z: z[1])	
	
    # Take the top 10 results
    results = sortedMovies.take(10)	
	
    # Print them out:
    for result in results:
        print(movieNames[result[0]], result[1])
