from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import *
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def main():
    spark = SparkSession.builder.master("local[2]").getOrCreate()
    df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/train.csv"). \
        withColumnRenamed("Survived", "label")
    avg_age = df.agg(mean("Age").alias("avg")).collect()[0]["avg"]
    na_map = {"Age": avg_age, "Sex": "male", "Embarked": "S", "Fare": 0.0, "SibSp": 0.0}
    df = df.na.fill(na_map)
    sex_indexer = StringIndexer().setInputCol(value="Sex").setOutputCol("sex_index")
    embarked_indexer = StringIndexer().setInputCol("Embarked").setOutputCol("embarked_index")
    vector_assembler = VectorAssembler().setInputCols(["sex_index", "embarked_index", "Fare", "SibSp", "Age"]). \
        setOutputCol("features")
    classifier = RandomForestClassifier().setFeaturesCol("features")

    pipeline = Pipeline().setStages([sex_indexer, embarked_indexer, vector_assembler, classifier])

    grid = ParamGridBuilder().addGrid(classifier.maxDepth, [5, 8]). \
        addGrid(classifier.maxBins, [16, 32]).build()

    crossval = CrossValidator(estimator=pipeline,
                              estimatorParamMaps=grid,
                              evaluator=BinaryClassificationEvaluator(),
                              numFolds=3)

    cv_model = crossval.fit(df)

    test_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/test.csv").fillna(na_map)
    cv_model.transform(test_df). \
        select("PassengerId", "prediction"). \
        show()


if __name__ == '__main__':
    main()
