import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Task2 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.FATAL)

    val spark_config = new SparkConf().setAppName("HW1").setMaster("local[2]")

    val spark_context = new SparkContext(spark_config)

    //var inputPath = "C:/Users/ordaz/OneDrive/Desktop/hw1/survey_results_public.csv"
    //var outputPath = "C:/Users/ordaz/OneDrive/Desktop/hw1/"

    var inputPath = args(0)
    var outputPath = args(1)

    /////////////////////////////////////////////
    val spark = SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;

    import spark.implicits._

    val df = spark.read.format("csv").load("file:///" + inputPath).toDF()


    val standardClean = df.filter(df("_c52") !== "NA").filter(df("_c52") !== "0")

    val standard = standardClean.select("_c3", "_c52").toDF("country", "salary")



    val standarded = standard.repartition(2)
    val standardMap = standarded.rdd.map(x => (x,1))


    val t2 = System.nanoTime()
    val out = standardMap.reduceByKey((a,b) => a+b).collect()
    val durationWithOut = (System.nanoTime() - t2)


    val partitioned = standard.repartition(2, $"country")
    val partitionedMap = partitioned.rdd.map(x => (x,1))


    val t1 = System.nanoTime()
    val outPartitioned = partitionedMap.reduceByKey((a,b) => a+b).collect()
    val durationWith = (System.nanoTime() - t1)

    import java.util.concurrent.TimeUnit


    val durationInMsStandard = TimeUnit.NANOSECONDS.toMillis(durationWithOut)
    val durationInMsPartitioned = TimeUnit.NANOSECONDS.toMillis(durationWith)


    /////////////////////get standard number of items per partition
    val standardItems = standarded.rdd.mapPartitionsWithIndex{case (i,rows) => Iterator((rows.size))}.toDF()
    val standardRdd = standardItems.rdd
    val standardNumberPartitions = standardRdd.map(x => x).collect()


    ////////////////////get partitioned number of items per partition
    val partitionItems = partitioned.rdd.mapPartitionsWithIndex{case (i,rows) => Iterator((rows.size))}.toDF()
    val partitionRdd = partitionItems.rdd
    val partitionNumberPartitions = partitionRdd.map(x => x).collect()


    val outputDF = Seq(
      ("standard", standardNumberPartitions(0).getInt(0), standardNumberPartitions(1).getInt(0), durationInMsStandard),
      ("partition", partitionNumberPartitions(0).getInt(0), partitionNumberPartitions(1).getInt(0), durationInMsPartitioned)
    ).toDF()


    outputDF.coalesce(1).write.csv(outputPath + "Azamat_Ordabekov_task2.csv" )


  }

}
