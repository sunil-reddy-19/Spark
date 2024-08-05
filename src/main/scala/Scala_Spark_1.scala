import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame
import org.apache.spark.sql.{Dataset, SparkSession,Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, explode, explode_outer}
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}



object Scala_Spark_1 {

  def spark_init(): SparkSession = {
     val spark = SparkSession.builder()
       .appName("Scala_Spark")
       .master("local[1]")
       .getOrCreate()
    spark
  }

  def read_file(fileName:String,spark:SparkSession) = {
    val df = spark.read.option("header","true").option("inferSchema","true").csv(fileName)
    df
  }

  def main(args:Array[String]): Unit = {



    val spark = spark_init()

    import spark.implicits._

    val filename="/Users/sunil_reddy/Spark/input_data/datasets/dw_dataset/sales_1.csv"

    var df1 = read_file(filename,spark)

    df1.show()

    val df_grp = df1.groupBy("date_of_sale")
      .agg(sum("total_amount")
        .alias("total_sum_by_date"))

    df_grp.show()

    var df_2 = df1.withColumn("dates",date_format(col("date_of_sale"),"dd/mm/yyyy"))

    //df_2.select(date_format(col("date_of_sale"),"MM-dd-yyyy").as("date_1")).show()

    df_2.show()

    df_2.printSchema()

    val arrayData = Seq(
      Row("James",List("Java","Scala"),Map("hair"->"black","eye"->"brown")),
      Row("Michael",List("Spark","Java",null),Map("hair"->"brown","eye"->null)),
      Row("Robert",List("CSharp",""),Map("hair"->"red","eye"->"")),
      Row("Washington",null,null),
      Row("Jefferson",List(),Map())
    )

    val arraySchema = new StructType()
      .add("name",StringType)
      .add("knownLanguages", ArrayType(StringType))
      .add("properties", MapType(StringType,StringType))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(arrayData),arraySchema)

    df.show()

    val df11 = df.select($"name",explode($"knownLanguages").as("knownLanguages"),$"properties")
    df1.show()
    val df2 = df11.select($"name",$"knownLanguages",explode($"properties"))
    //val df3 = df2.withColumnRenamed("key","attributte").withColumnRenamed("value","color")
    val df3 = df2.withColumnsRenamed(Map("key"->"attributte","value"->"color"))
    println("df3 data ")
    df3.show()

    df11.select($"name",$"knownLanguages",$"properties.hair".as("hair_color"),$"properties.eye".as("eye_color")).show()








  }

}
