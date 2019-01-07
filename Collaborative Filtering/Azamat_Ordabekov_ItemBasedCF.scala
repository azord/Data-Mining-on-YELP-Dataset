import java.io.{File, PrintWriter}

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD


object ItemBasedCF{
  Logger.getLogger("org").setLevel(Level.FATAL)
  val sparkConf = new SparkConf().setAppName("ItemBasedCF").setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  val neighbourhoodSize = 90


  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("ItemBasedCF")
      .config("spark.master", "local")
      .getOrCreate()


  import spark.implicits._

  def main(args: Array[String]): Unit = {
    //val inputFileData = "C:/Users/ordaz/OneDrive/Desktop/INF 553/HW/HW2/hw2/Data/train_review.csv"
    val inputFileData = args(0)
    val inputFileTest = args(1)
    //val inputFileTest = "C:/Users/ordaz/OneDrive/Desktop/INF 553/HW/HW2/hw2/Data/test_review.csv"


    val t1 = System.nanoTime

    val data = sc.textFile(inputFileData)

    val train_header = data.first()
    val firstCleaned = data.filter(partitionsOfMap => partitionsOfMap != train_header)
    val trainingPrepared = firstCleaned.map(_.split(','))




    val dev_test = sc.textFile(inputFileTest)
    val test_header = dev_test.first()
    var firstCleanedTest = dev_test.filter(partitionsOfMap => partitionsOfMap != test_header)
    var testing = firstCleanedTest.map(_.split(','))

    val getSize = testing.collect()

    //println("get size: " + getSize.size)


    val test1 = testing
    val data1 = trainingPrepared

    val merged = test1 ++ data1


    val userIdToInt = merged.map(row => row(0)).distinct.sortBy(x => x).zipWithUniqueId().collectAsMap
    val businessIdToInt = merged.map(row => row(1)).distinct.sortBy(x => x).zipWithUniqueId().collectAsMap



    val checkModelData  = testing.collect().map(d=>((userIdToInt(d(0)).toInt,businessIdToInt(d(1)).toInt),d(2).toDouble))
    val trainingMapData = trainingPrepared.collect().map(d=>((userIdToInt(d(0)).toInt,businessIdToInt(d(1)).toInt),d(2).toDouble))



    var itemSimilarities : scala.collection.mutable.Map[(Int,Int),Double] = scala.collection.mutable.Map[(Int,Int),Double]()


    def getavStarsByItem(rated_businesss: Iterable[Double]): Double = {
      val numberOfbusinesssRated = rated_businesss.size
      var cumulative_sum_of_ratings = 0.00

      rated_businesss.map(stars => {
        cumulative_sum_of_ratings += stars
      })

      val avg = cumulative_sum_of_ratings / numberOfbusinesssRated

      return avg
    }


    val trainingMapData_map = trainingMapData.toMap

    def getavStars(user: Int, rated_businesss: Iterable[Int]): Double = {
      val numberOfbusinesssRated = rated_businesss.size
      var cumulative_sum_of_ratings = 0.00
      rated_businesss.map(business => {
        cumulative_sum_of_ratings += trainingMapData_map(user, business)
        //cumulative_sum_of_ratings += trainingMapData_map1(user, business)
      })
      val avg = cumulative_sum_of_ratings / numberOfbusinesssRated
      return avg
    }


    //////////////////////////////////////////////////////////////////////////group by business and store values of stars
    val businessStars = trainingMapData.map(partitionsOfMap=>(partitionsOfMap._1._2,partitionsOfMap._2))
    val businessStarsRDD = sc.parallelize(businessStars).groupByKey()
    val businessStarsMap = businessStarsRDD.collectAsMap()


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    val businessUser = trainingMapData.map(partitionsOfMap=>(partitionsOfMap._1._2,partitionsOfMap._1._1))
    val businessUserRDD = sc.parallelize(businessUser).groupByKey()
    val businessUserMap = businessUserRDD.collectAsMap()



    def getWeigths(item1:Int, item2: Int) : Double = {

      var rU = item1
      var rV = item2


      if(itemSimilarities.contains((rU,rV))){
        return itemSimilarities((rU,rV))
      }

      if(!businessUserMap.contains(item1)){
        itemSimilarities((item1,item2)) = 0.0
        return 0.0
      }
      val usersRatedItem1 = businessUserMap(item1).toSet
      val usersRatedItem2 = businessUserMap(item2).toSet

      val bothRatedUsers = usersRatedItem1.intersect(usersRatedItem2)

      var division = 0.0

      var divisioner1 = 0.0
      var divisioner2 = 0.0

      bothRatedUsers.map(user=>{
        division += trainingMapData_map((user,item1)) * trainingMapData_map((user,item2))
        divisioner1 += trainingMapData_map((user,item1))
        divisioner2 += trainingMapData_map((user,item2))
      })


      var weight = 0.00
      if(division > 0.00 && (divisioner1 * divisioner2) > 0.00){
        weight = division/(divisioner1 * divisioner2)
      }
      itemSimilarities((rU,rV)) = weight
      return weight
    }


    val userBusiness = trainingMapData.map(partitionsOfMap=>partitionsOfMap._1)
    val userBusinessRDD = sc.parallelize(userBusiness).groupByKey()
    val userBusinessMap = userBusinessRDD.collectAsMap()



    val userAverage = userBusinessRDD.map(partitionsOfMap => {


      val curr_user = partitionsOfMap._1
      val bRatedTogether = partitionsOfMap._2
      val avStars = getavStars(curr_user, bRatedTogether)
      (curr_user, avStars)

    }).collectAsMap()


    val testing_filtered = checkModelData.filter(partitionsOfMap => userBusinessMap.contains(partitionsOfMap._1._1))
    val ColdStartData = checkModelData.filterNot(partitionsOfMap => userBusinessMap.contains(partitionsOfMap._1._1))
    //val testing_filtered = checkModelData

    //println("Size before predictionAza: " + checkModelData.size)
    //println("Size before prediction: " + testing_filtered.size)

    val predictionWithModel = testing_filtered.map(partitionsOfMap=> {

      val user = partitionsOfMap._1._1
      val business = partitionsOfMap._1._2

      val itemsRated = userBusinessMap(user)


      val tempSim = itemsRated.map(item => {
        val similarity = getWeigths(business, item)
        (item, similarity)
      })

      var division = 0.0
      var divider = 0.0


      val businessNeighborhood = tempSim.toList.sortBy(partitionsOfMap => partitionsOfMap._2).take(neighbourhoodSize)

      businessNeighborhood.map(partitionsOfMap => {

        var averageOfItemComparative = 0.00
        businessStarsMap.get(business).map { path =>
          val itemWithStars = businessStarsMap(business)
          averageOfItemComparative = getavStarsByItem(itemWithStars)
        } getOrElse {
          averageOfItemComparative = 0.00
        }
        //println(partitionsOfMap._1 + " " +  partitionsOfMap._2)
        division += (trainingMapData_map(user, partitionsOfMap._1) - averageOfItemComparative) * partitionsOfMap._2
        divider += math.abs(partitionsOfMap._2)

      })


      var averageOfItem = 0.00

      businessStarsMap.get(business).map { path =>
        val itemWithStars = businessStarsMap(business)
        averageOfItem = getavStarsByItem(itemWithStars)
      } getOrElse {
        averageOfItem = 0.00
      }

      var predicted_rating = 0.00
      if(division > 0.toDouble && divider > 0.toDouble){
        predicted_rating = averageOfItem +  division/divider
        val sum = businessNeighborhood.map(x=>x._2).sum
        val avgSim = sum/businessNeighborhood.size

      }
      else{
        userAverage.get(user).map { path =>
          predicted_rating = userAverage(user)
        } getOrElse {
          businessStarsMap.get(business).map { path =>
            val itemWithStars = businessStarsMap(business)
            predicted_rating = getavStarsByItem(itemWithStars)
          } getOrElse {
            predicted_rating = 3.00
          }
        }

      }

      if (predicted_rating  > 5.00) {
        predicted_rating = 5.00
      } else if (predicted_rating < 1.00) {
        predicted_rating = 1.0
      }

      (partitionsOfMap._1,partitionsOfMap._2,predicted_rating)
    })



    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    val predictionWithModelClean = ColdStartData.map(partitionsOfMap=> {

      var user = partitionsOfMap._1._1
      var business = partitionsOfMap._1._2
      var predictedClean = 0.00

      userAverage.get(user).map { path =>
        predictedClean = userAverage(user)
      } getOrElse {
        businessStarsMap.get(business).map { path =>
          val itemWithStars = businessStarsMap(business)
          predictedClean = getavStarsByItem(itemWithStars)
        } getOrElse {
          predictedClean = 3.00
        }
      }

      if (predictedClean  > 5.00) {
        predictedClean = 5.00
      } else if (predictedClean < 1.00) {
        predictedClean = 1.0
      }

      (partitionsOfMap._1,partitionsOfMap._2,predictedClean)
    })


    val fullpredictionWithModel = predictionWithModel ++ predictionWithModelClean

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


    val rddToCompareWithTest = sc.parallelize(fullpredictionWithModel)



    val range1 = rddToCompareWithTest.filter(partitionsOfMap => (math.abs(partitionsOfMap._2 - partitionsOfMap._3) >= 0.toDouble && math.abs(partitionsOfMap._2 - partitionsOfMap._3) < 1.toDouble)).count()
    val range2 = rddToCompareWithTest.filter(partitionsOfMap => (math.abs(partitionsOfMap._2 - partitionsOfMap._3) >= 1.toDouble && math.abs(partitionsOfMap._2 - partitionsOfMap._3) < 2.toDouble)).count()
    val range3 = rddToCompareWithTest.filter(partitionsOfMap => (math.abs(partitionsOfMap._2 - partitionsOfMap._3) >= 2.toDouble && math.abs(partitionsOfMap._2 - partitionsOfMap._3) < 3.toDouble)).count()
    val range4 = rddToCompareWithTest.filter(partitionsOfMap => (math.abs(partitionsOfMap._2 - partitionsOfMap._3) >= 3.toDouble && math.abs(partitionsOfMap._2 - partitionsOfMap._3) < 4.toDouble)).count()
    val range5 = rddToCompareWithTest.filter(partitionsOfMap => (math.abs(partitionsOfMap._2 - partitionsOfMap._3) >= 4.toDouble)).count()

    //rddToCompareWithTest.foreach(println)


    val sss = rddToCompareWithTest.coalesce(1).map(value =>
      (userIdToInt.find(_._2== value._1._1.toLong).map(_._1).get,
        businessIdToInt.find(_._2== value._1._2.toLong).map(_._1).get,
        value._2,
        value._3
      )
    )


    val predictionWithModelRange5 = sss.filter(partitionsOfMap => (math.abs(partitionsOfMap._3 - partitionsOfMap._4) >= 4.toDouble))


    //predictionWithModelRange5.foreach(println)

    val predictionWithModelRDD = sss.map(partitionsOfMap => (partitionsOfMap._1, partitionsOfMap._2, partitionsOfMap._4)).sortBy(value => (value._1, value._2), true).collect()

    //val map: Map[(String, String), Double] = predictionWithModelRDD.map(partitionsOfMap => (partitionsOfMap(0), partitionsOfMap(1)))

    //predictionWithModel

    //val ddd = findDifferenceOriginal

    val aaa  = testing.collect().map(d=>((d(0),d(1)),d(2).toDouble)).toMap

    val ttt = predictionWithModelRDD.groupBy(identity).map{case (x,y) => (x._1,x._2) -> x._3}

    val difference = aaa.filterNot(x => ttt.contains(x._1))

    //println("The difference: " + difference.size)




     val pw = new PrintWriter(new File("Azamat_Ordabekov_ItemBasedCF.txt" ))
     predictionWithModelRDD.foreach(x => pw.write((x._1.toString + ", " + x._2.toString + ", " + x._3.toString +  "\n")))
     pw.close()



    val rrr: RDD[String] = sss.coalesce(1).map(value => value.toString())


    //val valRDD = rrr.map(_.split(',')).collect()

    //val eee = sss.toDF()

    //eee.coalesce(1).write.csv("C:/Users/ordaz/OneDrive/Desktop/INF 553/HW/HW2/hw2/Data/mapKeyValueTask2.csv")



    println(">=0 and <1: " + range1)
    println(">=1 and <2: " + range2)
    println(">=2 and <3: " + range3)
    println(">=3 and <4: " + range4)
    println(">=4: " + range5)
    val RMSE = math.sqrt(rddToCompareWithTest.map(x => {(x._2 - x._3) * (x._2 - x._3)}).mean())


    println("RMSE: " + RMSE)


    val duration = (System.nanoTime - t1) / 1e9d
    println("The time " + duration + " sec")
  }


}