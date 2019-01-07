import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.log4j.{Level, Logger}
import java.io.{File, PrintWriter}


object ModelBasedCF {

  Logger.getLogger("org").setLevel(Level.FATAL)
  val sparkConf = new SparkConf().setAppName("ModelBasedCF").setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)


  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("ModelBasedCF")
      .config("spark.master", "local")
      .getOrCreate()



  def main(args: Array[String]): Unit = {
    //val inputFileData = "C:/Users/ordaz/OneDrive/Desktop/INF 553/HW/HW2/hw2/Data/train_review.csv"
    val inputFileData = args(0)
    val inputFileTest = args(1)
    //val inputFileTest = "C:/Users/ordaz/OneDrive/Desktop/INF 553/HW/HW2/hw2/Data/test_review.csv"



    val dataTraining = sc.textFile(inputFileData)
    val dataTest = sc.textFile(inputFileTest)

    val header = dataTraining.first()
    val withoutHeader = dataTraining.filter(row => row != header)

    val headerTest = dataTest.first()
    val withoutHeaderTest = dataTest.filter(row => row != headerTest)

    val test = withoutHeaderTest.map(_.split(','))
    val ratings = withoutHeader.map(_.split(','))



    val convertedTest = test.collect().map(x => (x(0).toString, x(1).toString)).toMap
    val convertedRating = ratings.collect().map(d=>(d(0).toString,d(1).toString)).toMap

    val difference = convertedTest.filterNot(x => convertedRating.contains(x._1))

    val coldStart = test.filter(x => difference.contains(x(0)))

    //val coldDF = coldStart.map(x => x(0)).toDF()


    val getDifferenceSize = coldStart.map(row => row).collect()
    //println(getDifferenceSize.size)

    //println(difference.size)

    //val diffToDF = difference.toSeq.toDF()


    val merged = ratings ++ test


    val userIdToInt = merged.map(row => row(0)).distinct.sortBy(x => x).zipWithUniqueId().collectAsMap
    val businessIdToInt = merged.map(row => row(1)).distinct.sortBy(x => x).zipWithUniqueId().collectAsMap

    //val rrrr = userIdToInt.toSeq.toDF()




    val ratingsRDD = ratings.map(row => {
      val user = userIdToInt(row(0)).toInt
      val business = businessIdToInt(row(1)).toInt
      val stars = row(2)
      Rating(user, business, stars.toDouble)
    })


    //val cleanTest = test.subtract(difference).union(test.subtract(difference))

    //val dept = difference.collect.toSet
    //val cleanTest = test.filter{row => !dept.contains(row(0))}

    //cleanTest.collect().foreach(arr => println(arr.mkString(", ")))



    val testingRDD = test.map( row => {
      val user = userIdToInt(row(0)).toInt
      val business = businessIdToInt(row(1)).toInt
      val stars = row(2)
      Rating(user, business, stars.toDouble)
    })

    val rank = 2
    val numIterations = 20
    val lambda = 0.22


      val model = new ALS().setIterations(numIterations) .setBlocks(-1)
        .setLambda(lambda)
        .setRank(rank)
        .setNonnegative(true)
        .run(ratingsRDD)


    val data: RDD[Rating] = testingRDD
    val modelForPrediction: MatrixFactorizationModel = model


    val predictions: RDD[Rating] = modelForPrediction.predict(data.map(x => (x.user, x.product)))


    var filteredPrediction = predictions.map(elem => {
      if(elem.rating > 5.00){
        Rating(elem.user, elem.product, 5.00)
      }
      else if (elem.rating < 1.00) {
        Rating(elem.user, elem.product, 1.00)
      }
      else{
        Rating(elem.user, elem.product, elem.rating)
      }
    })


    val predictionsAndRatings = filteredPrediction.map { x => ((x.user, x.product), x.rating) } .join(data.map(x => ((x.user, x.product), x.rating))).values


    //val tester = predictions.toDF()
    //val tester1 = data.toDF()



    val diff1 = predictionsAndRatings.filter(x=>( math.abs(x._1 - x._2) >= 0.00 && math.abs(x._1 - x._2) < 1.00 )).count()
    val diff2 = predictionsAndRatings.filter(x=>( math.abs(x._1 - x._2) >= 1.00 && math.abs(x._1 - x._2) < 2.00 )).count()
    val diff3 = predictionsAndRatings.filter(x=>( math.abs(x._1 - x._2) >= 2.00 && math.abs(x._1 - x._2) < 3.00 )).count()
    val diff4 = predictionsAndRatings.filter(x=>( math.abs(x._1 - x._2) >= 3.00 && math.abs(x._1 - x._2) < 4.00 )).count()
    val diff5 = predictionsAndRatings.filter(x=>( math.abs(x._1 - x._2) >= 4.00)).count()

    val total = diff1 + diff2 + diff3 + diff4 + diff5

    //println("Total " + total)

    println(">=0 and <1: " + diff1)
    println(">=1 and <2: " + diff2)
    println(">=2 and <3: " + diff3)
    println(">=3 and <4: " + diff4)
    println(">=4: " + diff5)


    //val outputTest: RDD[String] = filteredPrediction.map{x => (x.user, x.product, x.rating).toString}

    //val valRDD = outputTest.coalesce(1).map(_.split(',')).collect()


    val sss = filteredPrediction.map(value =>
      (userIdToInt.find(_._2== value.user.toLong).map(_._1).get,
        businessIdToInt.find(_._2== value.product.toLong).map(_._1).get,
        value.rating
        )
    )



    //valRDD.foreach(println)

    //val qqq = valRDD.toSeq.toDF()

    //mapRDD.toDF().show()

    //mapRDD.collect().foreach(println)

    //sss.foreach(println)


    val rmse = math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())


    val sorted = sss.sortBy(value => (value._1, value._2), true).collect()


    //sorted.take(10).foreach(println)

    val pw = new PrintWriter(new File("Azamat_Ordabekov_ModelBasedCF.txt" ))

    sorted.foreach(x => pw.write((x._1.toString + ", " + x._2.toString + ", " + x._3.toString +  "\n")))

    pw.close()

    println("RMSE: " + rmse)
  }

}

