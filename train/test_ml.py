from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder \
    .appName("SalaryPrediction") \
    .getOrCreate()

data = spark.read.csv("./jobs.csv", header=True, inferSchema=True, sep=";", multiLine=True)


data.show()
data.printSchema()

data.select("description").show()
data.select("sal_high").show()

data = data.withColumn("mean_salary", (col("sal_high") + col("sal_low")) / 2)

df = data.select(col('description'),
                 col('mean_salary').alias('salary'))

df = df.dropna()
df.select('salary').show()
df.select('description').show(truncate=False)

tokenizer = Tokenizer(inputCol="description", outputCol="words")
# Convert words to numerical vectors using TF-IDF
hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="rawFeatures")
idf = IDF(inputCol=hashingTF.getOutputCol(), outputCol="features")
# Define the regression model
rf = RandomForestRegressor(featuresCol="features", labelCol="salary")
# Define the pipeline
pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, rf])
# Split the data into training and testing sets
train_data, test_data = df.randomSplit([0.8, 0.2], seed=123)
# Train the model
model = pipeline.fit(train_data)

model.save("./")

