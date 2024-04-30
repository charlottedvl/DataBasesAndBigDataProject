import os
from pyspark.sql import SQLContext

DATALAKE_ROOT_FOLDER = "./datalake/"


def combine_data(current_day):
    RATING_PATH = DATALAKE_ROOT_FOLDER + "formatted/imdb/MovieRating/" + current_day + "/"
    USAGE_OUTPUT_FOLDER_STATS = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/MovieStatistics/" + current_day + "/"
    USAGE_OUTPUT_FOLDER_BEST = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/MovieTop10/" + current_day + "/"
    if not os.path.exists(USAGE_OUTPUT_FOLDER_STATS):
        os.makedirs(USAGE_OUTPUT_FOLDER_STATS)
    if not os.path.exists(USAGE_OUTPUT_FOLDER_BEST):
        os.makedirs(USAGE_OUTPUT_FOLDER_BEST)

    from pyspark import SparkContext

    sc = SparkContext(appName="CombineData")
    sqlContext = SQLContext(sc)
    df_ratings = sqlContext.read.parquet(RATING_PATH)
    df_ratings.registerTempTable("ratings")
    # Check content of the DataFrame df_ratings:
    print(df_ratings.show())
    stats_df = sqlContext.sql("SELECT AVG(averageRating) AS avg_rating,"
                              " MAX(averageRating) AS max_rating,"
                              " MIN(averageRating) AS min_rating,"
                              " COUNT(averageRating) AS count_rating"
                              " FROM ratings LIMIT 10")

    top10_df = sqlContext.sql("SELECT tconst, averageRating"
                              " FROM ratings"
                              " WHERE numVotes > 50000 "
                              " ORDER BY averageRating DESC"
                              " LIMIT 10")

    print(stats_df.show())
    # Check content of the DataFrame stats_df and save it:
    stats_df.write.save(USAGE_OUTPUT_FOLDER_STATS + "res.snappy.parquet",
                        mode="overwrite")

    print(top10_df.show())
    stats_df.write.save(USAGE_OUTPUT_FOLDER_BEST + "res.snappy.parquet",
                        mode="overwrite")
