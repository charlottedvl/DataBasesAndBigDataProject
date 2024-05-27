
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

data = spark.read.csv("test_data.csv", header=True, inferSchema=True, sep=",", multiLine=True)

data = data.withColumn("calc_salary", col("avg_salary").cast("double") * 1000)
df = data.select(col('Job Description').alias('description'), col('calc_salary').alias('salary'))
df = df.dropna()

df = df.withColumn("salary", when(col("salary") > 400000, 400000).otherwise(col("salary")))

df.show()

loaded_model = PipelineModel.load("./random_forest/ensemble_model")

predictions = loaded_model.transform(df)

predictions.select('words').show(truncate=False)

evaluator = RegressionEvaluator(labelCol="salary", predictionCol="prediction", metricName="rmse")

rmse = evaluator.evaluate(predictions)

print("Root Mean Squared Error (RMSE) on test data = {:.2f}".format(rmse))

predicted_salaries = predictions.select('prediction').rdd.flatMap(lambda x: x).collect()

actual_salaries = df.select('salary').rdd.flatMap(lambda x: x).collect()

"""
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


"""


predictions_df = predictions.select("salary", "prediction").toPandas()

plt.figure(figsize=(10, 6))
plt.scatter(predictions_df["salary"], predictions_df["prediction"], alpha=0.5)
plt.plot([0, 250000], [0, 250000], color='red', linestyle='--')  # Plotting the line y = x
plt.xlim(0, 250000)
plt.ylim(0, 250000)
plt.xlabel("Actual Salary")
plt.ylabel("Predicted Salary")
plt.title("Actual vs Predicted Salary")
plt.grid(True)
plt.show()
