# Databricks notebook source
from pyspark.ml import Pipeline
from pyspark.ml.feature import IndexToString,StringIndexer, VectorIndexer
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import VectorIndexer
from mining.decision_tree import DecisionTree
from src.utils.utils import *

resource_id = string_to_list_with_spaces(dbutils.widgets.get("resourceId"))
resource_id = [f"file:/dbfs/FileStore/tables/tesis/transformed_data/{res}" for res in resource_id]

handler = DecisionTree(spark, dbutils)

schema = DecisionTree.generate_schema()

df = handler.integrate_csv(resource_id, schema=schema)

print("Readed File DataFrame")
display(df)

df = handler.group_categorical_and_continuous_Cols(df, "id", ["timeShift"], ["steps", "hour", "minute"], "stepsLvl")

print("Group independent and dependant variables")
display(df)

labelIndexer = StringIndexer(inputCol='label',
                        outputCol='indexedLabel').fit(df)

print("Labels Indexed")
labelIndexer.transform(df).show(5, True)


featureIndexer =VectorIndexer(inputCol="features", \
                            outputCol="indexedFeatures", \
                            maxCategories=4).fit(df)

print("Features Indexed")
featureIndexer.transform(df).show(5, True)

# Split the data into training and test sets (40% held out for testing)
(trainingData, testData) = df.randomSplit([0.7, 0.3])
dTree = DecisionTreeClassifier(labelCol='indexedLabel', featuresCol='indexedFeatures')

# Convert indexed labels back to original labels.
labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                            labels=labelIndexer.labels)

# Chain indexers and tree in a Pipeline
pipeline = Pipeline(stages=[labelIndexer, featureIndexer, dTree,labelConverter])
# Train model.  This also runs the indexers.
model = pipeline.fit(trainingData)

# Make predictions.
predictions = model.transform(testData)
# Select example rows to display.
predictions.select("features","label","predictedLabel").show(500)
