from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.sql.functions import col
from handlers.common_handler import CommonHandler

class DecisionTree(CommonHandler):

    
    def group_categorical_and_continuous_Cols(self, df, indexCol, categoricalCols, continuousCols, labelCol):
        indexers = [ StringIndexer(inputCol=c, outputCol="{0}_indexed".format(c))
                    for c in categoricalCols ]

        # default setting: dropLast=True
        encoders = [ OneHotEncoder(inputCol=indexer.getOutputCol(),
                    outputCol="{0}_encoded".format(indexer.getOutputCol()))
                    for indexer in indexers ]

        assembler = VectorAssembler(inputCols=[encoder.getOutputCol() for encoder in encoders]
                                    + continuousCols, outputCol="features")

        pipeline = Pipeline(stages=indexers + encoders + [assembler])

        model=pipeline.fit(df)
        data = model.transform(df)

        data = data.withColumn('label',col(labelCol))

        return data.select(indexCol,'features','label')
