import os
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_utc_timestamp, concat_ws, regexp_replace, explode, split, desc, collect_list, \
    create_map, lit, concat, monotonically_increasing_id, lower

from bertopic import BERTopic
from bertopic.representation import KeyBERTInspired
from sklearn.datasets import fetch_20newsgroups
from sparknlp.base import *
from sparknlp.annotator import *
from sparknlp.pretrained import PretrainedPipeline
import sparknlp
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer, VectorAssembler
from pyspark.ml.clustering import LDA
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
import matplotlib.pyplot as plt
import seaborn as sns

import pyspark
import string
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.mllib.util import MLUtils
from pyspark.sql.types import *
from pyspark.ml.feature import CountVectorizer, CountVectorizerModel, Tokenizer, RegexTokenizer, StopWordsRemover

datalake_root_folder = "./datalake/"

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


def combine_data(current_day):
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
    predicted_df.write.save(parquet_file_name, mode="overwrite")



def tokenize(data_df):
    from pyspark.ml import PipelineModel
    loaded_model = PipelineModel.load("../train/random_forest/ensemble_model")
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
