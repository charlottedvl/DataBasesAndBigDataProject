from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder \
    .appName("SalaryPrediction") \
    .getOrCreate()

"""
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
"""

data = spark.read.csv("test_data.csv", header=True, inferSchema=True, sep=",", multiLine=True)

data = data.withColumn("calc_salary", col("avg_salary").cast("double") * 1000)
df = data.select(col('Job Description').alias('description'), col('calc_salary').alias('salary'))
df = df.dropna()

df = df.withColumn("salary", when(col("salary") > 400000, 400000).otherwise(col("salary")))
df = df.withColumn("salary", when(col("salary") < 40000, 40000).otherwise(col("salary")))


chunk_size = 50
num_chunks = df.count() // chunk_size

# Define the pipeline outside the loop
tokenizer = Tokenizer(inputCol="description", outputCol="words")

hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="rawFeatures")
idf = IDF(inputCol=hashingTF.getOutputCol(), outputCol="features")
rf = RandomForestRegressor(featuresCol="features", labelCol="salary")
pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, rf])

"""
remover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="filtered_words")
cv = CountVectorizer(inputCol=remover.getOutputCol(), outputCol="features")
gbt = GBTRegressor(featuresCol="features", labelCol="salary")
pipeline = Pipeline(stages=[tokenizer, remover, cv, gbt])
"""

"""
remover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="filtered_words")
cv = CountVectorizer(inputCol=remover.getOutputCol(), outputCol="features")
dt = DecisionTreeRegressor(featuresCol="features", labelCol="salary")
pipeline = Pipeline(stages=[tokenizer, remover, cv, dt])
"""

"""
remover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="filtered_words")
cv = CountVectorizer(inputCol=remover.getOutputCol(), outputCol="features")
elastic_net = LinearRegression(featuresCol="features", labelCol="salary", elasticNetParam=0.5, regParam=0.1)
pipeline = Pipeline(stages=[tokenizer, remover, cv, elastic_net])
"""

"""
remover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="filtered_words")
cv = CountVectorizer(inputCol=remover.getOutputCol(), outputCol="features")
iso_reg = IsotonicRegression(featuresCol="features", labelCol="salary")
pipeline = Pipeline(stages=[tokenizer, remover, cv, iso_reg])
"""

"""
remover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="filtered_words")
cv = CountVectorizer(inputCol=remover.getOutputCol(), outputCol="features")
scaler = StandardScaler(inputCol=cv.getOutputCol(), outputCol="scaled_features")
lasso_reg = LinearRegression(featuresCol="scaled_features", labelCol="salary", elasticNetParam=1.0, regParam=0.1)
pipeline = Pipeline(stages=[tokenizer, remover, cv, scaler, lasso_reg])
"""

for i in range(num_chunks):
    # Get a chunk of data
    start_index = i * chunk_size
    end_index = start_index + chunk_size
    chunk_df = df.limit(chunk_size)
    model = pipeline.fit(chunk_df)
    model_path = f"./random_forest2/random_forest_regressor_model_{i}"
    model.save(model_path)

