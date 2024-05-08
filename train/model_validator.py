
from pyspark.ml.evaluation import RegressionEvaluator

from pyspark.ml import PipelineModel

from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np


spark = SparkSession.builder \
    .appName("SalaryPrediction") \
    .getOrCreate()

data = spark.read.csv("jobs.csv", header=True, inferSchema=True, sep=";")
data = data.withColumn("mean_salary", (col("sal_high") + col("sal_low")) / 2)
df = data.select(col('description'), col('mean_salary').alias('salary'))
df = df.dropna()

df = df.withColumn("salary", col("salary").cast("double"))
df = df.withColumn("salary", when(col("salary") > 400000, 400000).otherwise(col("salary")))


df.show()

loaded_model = PipelineModel.load("./gbt/ensemble_model")

predictions = loaded_model.transform(df)

predictions.show()

evaluator = RegressionEvaluator(labelCol="salary", predictionCol="prediction", metricName="rmse")

rmse = evaluator.evaluate(predictions)

print("Root Mean Squared Error (RMSE) on test data = {:.2f}".format(rmse))

predicted_salaries = predictions.select('prediction').rdd.flatMap(lambda x: x).collect()

actual_salaries = df.select('salary').rdd.flatMap(lambda x: x).collect()


plt.figure(figsize=(10, 6))

bin_range = (min(predicted_salaries), max(predicted_salaries))

sns.histplot(predicted_salaries, bins=20, kde=True, color='blue', label='Predicted', binrange=bin_range)
sns.histplot(actual_salaries, bins=20, kde=True, color='red', label='Actual', binrange=bin_range)

plt.title('Salary Distribution')
plt.xlabel('Salary')
plt.ylabel('Frequency')
plt.xlim(0, 200000)
plt.legend()
plt.show()
