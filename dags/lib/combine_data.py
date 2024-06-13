import os
from pyspark import SparkContext
from pyspark.sql import SQLContext

datalake_root_folder = "datalake/"

"""
document_assembler = DocumentAssembler() \
    .setInputCol("clean_content") \
    .setOutputCol("document") \
    .setCleanupMode("shrink")

tokenizer = Tokenizer() \
    .setInputCols(["document"]) \
    .setOutputCol("token")

normalizer = Normalizer() \
    .setInputCols(["token"]) \
    .setOutputCol("normalized")

stopwords_cleaner = StopWordsCleaner() \
    .setInputCols("normalized") \
    .setOutputCol("cleanTokens") \
    .setCaseSensitive(False)

finisher = Finisher() \
    .setInputCols(["cleanTokens"]) \
    .setOutputCols(["tokens"]) \
    .setOutputAsArray(True) \
    .setCleanAnnotations(False)

"""


def combine_data(current_day, s3):

    formatted_path_findwork = datalake_root_folder + "formatted/findwork/job/" + current_day + "/offers.snappy.parquet"
    formatted_path_themuse = datalake_root_folder + "formatted/themuse/job/" + current_day + "/offers.snappy.parquet"

    sc = SparkContext(appName="CombineData")
    sqlContext = SQLContext(sc)
    df = sqlContext.read.parquet(formatted_path_findwork, formatted_path_themuse)
    df.registerTempTable("job")

    predicted_df = tokenize(df)

    predicted_df.printSchema()
    # new_df.show()

    parquet_file_name = datalake_root_folder + "combined/job/" + current_day + "/offers.snappy.parquet"
    save_as_parquet(predicted_df, parquet_file_name, s3)


def save_as_parquet(df, formatted_path, s3):
    df.write.save(formatted_path, mode="overwrite")
    s3.upload_directory(formatted_path)


def prediction_ml(data_df):
    from pyspark.ml import PipelineModel
    # Edit the path where the model is stored
    loaded_model = PipelineModel.load("/Users/random_forest/ensemble_model")
    predictions = loaded_model.transform(data_df)
    predictions.show()

    # Plotting the distribution
    """
    salary_list = predictions.select('prediction').rdd.flatMap(lambda x: x).collect()
    plt.figure(figsize=(10, 6))
    sns.histplot(salary_list, bins=20, kde=True, color='blue')
    plt.title('Salary Distribution')
    plt.xlabel('Salary')
    plt.ylabel('Frequency')
    plt.show()
    
    """

    return predictions


def combine_data_findwork(group_name, table_name, current_day, file_name):
    path = group_name + "/" + table_name + "/" + current_day + "/"

    formatted_path = datalake_root_folder + "formatted/" + path + file_name
    print(formatted_path)
    usage_stats = datalake_root_folder + "usage/jobAnalysis/jobStatistics/" + current_day + "/"
    usage_best = datalake_root_folder + "usage/jobAnalysis/jobTop10/" + current_day

    if not os.path.exists(usage_stats):
        os.makedirs(usage_stats)
    if not os.path.exists(usage_best):
        os.makedirs(usage_best)

    sc = SparkContext(appName="CombineData")
    sqlContext = SQLContext(sc)
    df = sqlContext.read.parquet(formatted_path)
    df.registerTempTable("job")
    # Check content of the DataFrame df_ratings:
    print(df.show())

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
    stats_df.write.save(usage_stats + "res.snappy.parquet",
                        mode="overwrite")

    print(top10_df.show())
    stats_df.write.save(usage_best + "res.snappy.parquet",
                        mode="overwrite")
