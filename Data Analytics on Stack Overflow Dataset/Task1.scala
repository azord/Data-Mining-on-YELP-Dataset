import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object Task1 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.FATAL)

    //var inputFile = "C:/Users/ordaz/OneDrive/Desktop/hw1/survey_results_public.csv"
    //var outputPath = "C:/Users/ordaz/OneDrive/Desktop/hw1/"

    var inputFile = args(0)
    var outputPath = args(1)

    val spark_config = new SparkConf().setAppName("HW1").setMaster("local[*]")

    val spark_context = new SparkContext(spark_config)


    /////////////////////////////////////////////
    val spark = SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;


    val someSchema = List(
      StructField("Total", StringType, true),
      StructField("Count", LongType, true)
    )


    val df = spark.read.format("csv").load("file:///" + inputFile).toDF()
    val df1 = df.filter(df("_c52") !== "NA").filter(df("_c52") !== "0").groupBy(df("_c3")).count().orderBy(asc("_c3"))


    val f = df1.drop("_c3").columns.map(col).reduce((c1, c2) => c1 + c2)

    val total = df1.agg(sum(f).alias("total"))

    val someData = Seq(
      Row("Total", total.head().getLong(0))
    )


    val someDF = spark.createDataFrame(
      spark_context.parallelize(someData),
      StructType(someSchema)
    )

    someDF.union(df1).coalesce(1).write.csv(outputPath + "Azamat_Ordabekov_task1.csv")

  }
}
