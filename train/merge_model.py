from pyspark.ml import PipelineModel

# Assuming num_chunks is defined as the number of chunks
# Load the first model
path = "./gbt/gbt_regressor"
ensemble_model = PipelineModel.load(f"{path}_model_0")

# Loop through the remaining models and merge them into the ensemble
for i in range(0, 41):
    print(i)
    model_path = f"{path}_model_{i}"
    model = PipelineModel.load(model_path)

    # Update the ensemble model by combining it with the new model
    ensemble_model.stages[-1].trees.extend(model.stages[-1].trees)

ensemble_model.save("./gbt/ensemble_model")
