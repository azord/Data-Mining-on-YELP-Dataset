import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{max, min}


object Task3 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.FATAL)

    val spark_config = new SparkConf().setAppName("HW1").setMaster("local[*]")
    val spark_context = new SparkContext(spark_config)


    //val inputPath = "C:/Users/ordaz/OneDrive/Desktop/hw1/survey_results_public.csv"
    //val outputPath = "C:/Users/ordaz/OneDrive/Desktop/hw1/"

    val inputPath = args(0)
    val outputPath = args(1)



    /////////////////////////////////////////////
    val spark = SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;


    val someSchema = List(
      StructField("Total", StringType, true),
      StructField("Count", LongType, true)
    )

    val df = spark.read.format("csv").load("file:///" + inputPath).toDF()
    val df1 = df.filter(df("_c52") !== "NA").filter(df("_c52") !== "0").filter(df("_c3") !== "Country")

    val withoutCommas = df1.withColumn("_c52", regexp_replace(df("_c52"), "\\,", ""))


    val transformation1 = withoutCommas.withColumn("_c52", when(col("_c53").equalTo("Monthly"), col("_c52") * 12 ).otherwise(col("_c52")))
    val transformation2 = transformation1.withColumn("_c52", when(col("_c53").equalTo("Weekly"), col("_c52") * 52 ).otherwise(col("_c52")))


    val df3 = transformation2.select("_c3", "_c52", "_c53")
    val df4 = df3.selectExpr("_c3", "cast(_c52 as int) _c52", "_c53")


    val lastTransformation = df4.groupBy(df3("_c3")).count().orderBy(asc("_c3"))
    val withAggregates = df4.groupBy(df3("_c3")).agg(min("_c52").as("Min Value"), max("_c52").as("Max value"), bround(avg("_c52"), 2))
    //withAggregates.withColumnRenamed("_c3", "Country")


    val outer_join = lastTransformation.join(withAggregates, "_c3")

    val lastLastTransformation = outer_join.orderBy("_c3")


    lastLastTransformation.coalesce(1).write.csv(outputPath + "Azamat_Ordabekov_task3.csv")
  }
}