from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("SalaryPrediction") \
    .getOrCreate()

data = spark.read.csv("jobs.csv", header=True, inferSchema=True, sep=";")

data.show()
data.printSchema()

data.select("description").show()
data.select("sal_high").show()

data = data.withColumn("mean_salary", (col("sal_high") + col("sal_low")) / 2)

df = data.select(col('description'), col('mean_salary').alias('salary'))

df = df.dropna()
df.select('salary').show()
df.select('description').show(truncate=False)

chunk_size = 50
num_chunks = df.count() // chunk_size

# Define the pipeline outside the loop
tokenizer = Tokenizer(inputCol="description", outputCol="words")
"""
hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="rawFeatures")
idf = IDF(inputCol=hashingTF.getOutputCol(), outputCol="features")
rf = RandomForestRegressor(featuresCol="features", labelCol="salary")
pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, rf])
"""

remover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="filtered_words")
cv = CountVectorizer(inputCol=remover.getOutputCol(), outputCol="features")
gbt = GBTRegressor(featuresCol="features", labelCol="salary")
pipeline = Pipeline(stages=[tokenizer, remover, cv, gbt])

for i in range(num_chunks):
    # Get a chunk of data
    start_index = i * chunk_size
    end_index = start_index + chunk_size
    chunk_df = df.limit(chunk_size)

    # Train the model on the chunk
    model = pipeline.fit(chunk_df)

    # Save the model
    model_path = f"./gbt/gbt_regressor_model_{i}"
    model.save(model_path)

