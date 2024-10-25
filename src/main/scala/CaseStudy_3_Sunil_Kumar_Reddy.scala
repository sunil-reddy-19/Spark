import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}


object CaseStudy_3_Sunil_Kumar_Reddy {

  val loanInputURL = "input_data/datasets/CaseStudyData_3/CaseStudy_3_Loan.csv"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("CaseStudy_3")
      .master("local[1]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("error")

    val rawDf = spark.read.option("header", "true").option("inferSchema", "true").csv(loanInputURL)
    val data_filled = rawDf.na.drop


    // Encode categorical variables
    val indexers = Seq("gender", "education_level", "marital_status","loan_status").map(col => new StringIndexer().setInputCol(col).setOutputCol(col + "_index"))
    val models = indexers.map(_.fit(data_filled)) // Fit each StringIndexer to the data
    val indexedData = models.foldLeft(data_filled) { (data_filled, model) => model.transform(data_filled) } // Transform data using the fitted models

    indexedData.show()

    val fieldsForTraining = indexedData.select("age", "gender_index", "education_level_index", "marital_status_index", "income", "credit_score","loan_status_index").withColumnRenamed("loan_status_index", "label")

    // Assemble features
    val assembler = new VectorAssembler().setInputCols(Array("age", "gender_index", "education_level_index", "marital_status_index", "income", "credit_score")).setOutputCol("features")
    val finalDf = assembler.transform(fieldsForTraining)

    // Split data into training and testing sets
    finalDf.show(false)
    finalDf.printSchema()
    val trainTestDf: Array[DataFrame] = finalDf.randomSplit(Array[Double](0.8, 0.2))
    val tainDf = trainTestDf(0)
    val testDf = trainTestDf(1)

    val lr: LogisticRegression = new LogisticRegression
    val trainedModel: LogisticRegressionModel = lr.fit(tainDf)
    println(trainedModel.coefficients)
    val testPredictionsDf = trainedModel.transform(testDf)
    testPredictionsDf.show()
    val evaluator: Double = new BinaryClassificationEvaluator().evaluate(testPredictionsDf)
    println("accuracy = " + evaluator)




    // Predict the probability for the new applicant
    val new_applicant = spark.createDataFrame(Seq(
      (51, 1.0, 3.0, 1.0, 90000, 750)
    )).toDF("age", "gender_index", "education_level_index", "marital_status_index", "income", "credit_score")

    val transformed_new_applicant = assembler.transform(new_applicant)
    val prediction = trainedModel.transform(transformed_new_applicant)
    val prediction_prob = prediction.first()


    println("Prediction Probability:", prediction_prob)










  }

}
