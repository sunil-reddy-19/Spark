object SparkML {

  import org.apache.spark.ml.evaluation.RegressionEvaluator
  import org.apache.spark.sql.{Dataset, Row, SparkSession}
  import org.apache.spark.ml.feature.VectorAssembler
  import org.apache.spark.ml.regression.LinearRegression

    val housepriceUrl = "/Users/sunil_reddy/Scala/dataset/Melbourne_house_price.csv"


    def main(args: Array[String]): Unit = {


      val spark = SparkSession.builder
        .appName("Simple Application")
        .master("local[1]")
        .getOrCreate()

      spark.sparkContext.setLogLevel("error")


      val rawDf = spark.read.option("header", "true").option("inferSchema", "true")
        .csv(housepriceUrl)
      //    rawDf.show(false)
      //    rawDf.printSchema()
      //
      //
      val requiredFields = rawDf.filter("Suburb='Abbotsford'")
        .drop("Suburb", "Address", "Method", "Type", "SellerG", "Date",
          "Postcode", "CouncilArea", "Lattitude", "Distance", "PropertyCount",
          "Longtitude", "RegionName")
      requiredFields.printSchema()

      val fieldsForTraining = requiredFields
        .na
        .drop.selectExpr("cast(Rooms as double)", "cast(Price as double)",
          "cast(Bedroom2 as double)", "cast(Bathroom as double)", "cast(Car as double)",
          "cast(Landsize as double)",
          "cast(BuildingArea as double)", "cast(YearBuilt as double)")
        .withColumnRenamed("Price", "label")
      //
      fieldsForTraining.show()
      fieldsForTraining.printSchema()
      val inputCols = Array[String]("Rooms", "Bedroom2", "Bathroom", "Car", "Landsize", "BuildingArea", "YearBuilt")
      val assembler = new VectorAssembler().setInputCols(inputCols).setOutputCol("features")
      val finalDf = assembler.transform(fieldsForTraining)
      finalDf.show()
      finalDf.printSchema()
      val trainTestDf = finalDf.randomSplit(Array[Double](0.8, 0.2))
      val trainDf = trainTestDf(0)
      val testDf = trainTestDf(1)
      //
      val lr = new LinearRegression()
      val trainedModel =  lr.fit(trainDf)
      println(trainedModel.coefficients)
      val testPredictionsDf = trainedModel.transform(testDf)
      testPredictionsDf.show()
      val evaluator = new RegressionEvaluator().setMetricName("rmse").evaluate(testPredictionsDf)
      println("RMSE = " + evaluator)

    }


}
