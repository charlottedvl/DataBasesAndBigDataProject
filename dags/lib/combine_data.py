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
    formatted_path_findwork = datalake_root_folder + "formatted/findwork/job/" + current_day + "/announce.snappy.parquet"
    formatted_path_themuse = datalake_root_folder + "formatted/themuse/job/" + current_day + "/announce.snappy.parquet"

    sc = SparkContext(appName="CombineData")
    sqlContext = SQLContext(sc)
    df = sqlContext.read.parquet(formatted_path_findwork, formatted_path_themuse)
    df.registerTempTable("job")

    tokenize(df)


def tokenize(data_df):
    # Tokenize the text in the clean_content column
    tokenizer = Tokenizer(inputCol="clean_content", outputCol="words")
    wordsDataFrame = tokenizer.transform(data_df)

    # Remove stopwords
    job_stopwords = ["job", "position", "experience", "skills", "work", "role", "company", "-", 'we&#x27;re']
    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    wordsDataFrame = remover.transform(wordsDataFrame)

    # Vectorize the filtered words
    #job_lexicon = ["engineer", "developer", "manager", "analyst", "consultant", "sales", "marketing", "designer", "specialist"]
    cv = CountVectorizer(inputCol="filtered", outputCol="vectors")
    cvmodel = cv.fit(wordsDataFrame)
    df_vect = cvmodel.transform(wordsDataFrame)

    # Train the LDA model
    lda = LDA(featuresCol='vectors')
    model = lda.fit(df_vect)

    # Get the topics
    topics = model.describeTopics()
    vocab = cvmodel.vocabulary

    # Tagging: Assign topics to each job offer
    tagged_data = model.transform(df_vect).select("id", "topicDistribution")

    # Show topics and tag each job offer with dominant topic
    topics.show(truncate=False)
    tagged_data.show()

    # Example of tagging each job offer with dominant topic
    for row in tagged_data.collect():
        dominant_topic_index = row.topicDistribution.argmax()
        print(f"Job Offer ID: {row.id}, Dominant Topic: {dominant_topic_index}")

    for row in topics.collect():
        topic_words = [vocab[idx] for idx in row.termIndices]
        print("Topic {}: {}".format(row.topic, topic_words))




def extract_proba(df):
    nlp_pipeline = Pipeline(
        stages=[document_assembler,
                tokenizer,
                normalizer,
                stopwords_cleaner,
                finisher])

    nlp_model = nlp_pipeline.fit(df)

    processed_df = nlp_model.transform(df)

    tokens_df = processed_df.select('tokens').limit(10000)

    cv = CountVectorizer(inputCol="tokens", outputCol="features", vocabSize=1000, minDF=3.0)

    cv_model = cv.fit(tokens_df)

    vectorized_tokens = cv_model.transform(tokens_df)

    num_topics = 15
    lda = LDA(k=num_topics, maxIter=10)
    model = lda.fit(vectorized_tokens)
    vocab = cv_model.vocabulary
    topics = model.describeTopics()
    topics_rdd = topics.rdd
    topics_words = topics_rdd \
        .map(lambda row: row['termIndices']) \
        .map(lambda idx_list: [vocab[idx] for idx in idx_list]) \
        .collect()

    def get_words(idx_list):
        return [vocab[idx] for idx in idx_list]

    udf_get_words = udf(get_words, ArrayType(StringType()))
    topics = topics.withColumn("words", udf_get_words(topics.termIndices))

    topics_df = topics.select("topic", "words")

    topics_df.show(truncate=False)


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
