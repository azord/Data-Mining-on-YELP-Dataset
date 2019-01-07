import java.io._

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import collection.{mutable}
import scala.collection.mutable.HashMap

object Azamat_Ordabekov_SON {
  val time = System.nanoTime
  Logger.getLogger("org").setLevel(Level.FATAL)

  def main(args: Array[String]): Unit = {
    val spark_config = new SparkConf().setAppName("HW3").setMaster("local[*]")


    val sc = new SparkContext(spark_config)



    //val inputRDD = sc.textFile("C:/Users/ordaz/OneDrive/Desktop/INF 553/HW/HW3/inf553_assignment3/Data/yelp_reviews_large" + ".txt")
    val inputRDD = sc.textFile(args(0))
    //val support = 120000
    val support = args(1).toInt
    //val outputPath = "C:/Users/ordaz/OneDrive/Desktop/hw3/Azamat_Ordabekov_SON_yelp_reviews_large_120000.txt"
    val outputPath = args(2)

    val writer = new BufferedWriter(new FileWriter(outputPath))

    var isFind = true





    /////////////////////////////////////single word count
    val single = inputRDD.mapPartitions(iter =>
      {
        iter.map(m2 => (m2.split(",")(0), m2.split(",")(1)))
      }
      , false).groupBy(_._2).mapValues(_.size)

      val filteredSingle = single.filter(_._2 >= support)



    /////////////////////////////////////////collect each word's review ids
    val revToMap = inputRDD.mapPartitions(iter =>
    {
      iter.map(s => (s.split(",")(0).toInt,s.split(",")(1)))
    }
      , false).groupBy(_._2).mapValues(_.map(_._1).toSet)

      if (filteredSingle.count().toInt == 0) {
        isFind = false
      }

    if (isFind) {


      /////////////////////////////////////////creates combinations for pair sets
      val forCombinations = filteredSingle.map(elem => elem._1).collect().toList
      val combinations = forCombinations.combinations(2)
      val combinationsRdd = sc.parallelize(combinations.toList)

      ////////////////////////////////////////////////////////////////sorting singles for output
      val sortingSingles = forCombinations.sortWith(_ < _)

      for (k <- sortingSingles) {
        writer.write("(" + k + "), ")
      }
      writer.write("\n")
      writer.write("\n")
      ////////////////////////////////////////////////////////////////
      val totalRows = combinationsRdd.count()

      val filteredRevMap = revToMap.filter{case (key, value) => forCombinations.contains(key)}


      val asMap = filteredRevMap.collectAsMap()



      val pairPartitioning = combinationsRdd.mapPartitions(iter => {
        val filteredHash: HashMap[Seq[String], Int] = new mutable.HashMap[Seq[String], Int]()
        var numberRows = 0


        for (k <- iter) {
          val listOfReviewsWord = asMap.get(k(0)).get
          val listOfReviewsWord1 = asMap.get(k(1)).get
          val intersect = listOfReviewsWord.intersect(listOfReviewsWord1)
          val tmpSeq = Seq(k(0), k(1))
          filteredHash.put(tmpSeq, intersect.size)
          numberRows += 1
        }

        val supportPerPartition = (numberRows/totalRows) * support
        val sliced = filteredHash.filter(elem => elem._2 >= supportPerPartition)

        sliced.toIterator
      },false).reduceByKey(_+_)


      val finalFiltered = pairPartitioning.filter(elem => elem._2 >= support)

      ///////////////////////for many combinations


      var forManySetCombinations = finalFiltered.map(elem => elem._1).collect().toSeq

      ///////////////////////////////////////////////print pair sets
      val sortingPairs = forManySetCombinations.sortBy(r => (r(0), r(1)))

      for (k <- sortingPairs) {
        val cutString = k.toString().replaceAll("List", "")
        writer.write(cutString + ", ")
      }
      writer.write("\n")
      writer.write("\n")
      ///////////////////////////////////////////////////////////////


      var combinationNumber = 3

      while (isFind) {

        val multipleSets: HashMap[Seq[String], Int] = new mutable.HashMap[Seq[String], Int]()
        var b = mutable.MutableList[String]()

        for (k <- forManySetCombinations) {
          for (i <- k) {
            if (!b.contains(i)) {
              b = b :+ i
            }
          }
        }

        val forNext = b.combinations(combinationNumber).toSeq
        val forTripleCombinations1 = sc.parallelize(forNext)


        val values = forTripleCombinations1.mapPartitions(iter => {
          var numberRows = 0
          for (k1 <- iter) {
            var allIntersect = scala.collection.Set[Int]()
            var countWord = 0
            for (k <- k1) {
              val listOfReviewsWord = asMap.get(k).get
              if (!allIntersect.isEmpty) {
                allIntersect = allIntersect.intersect(listOfReviewsWord)

              } else if (countWord == 0) {
                allIntersect = listOfReviewsWord
              } else {
                allIntersect = Set(0)
              }
              countWord += 1
            }

            numberRows += 1
            multipleSets.put(k1, allIntersect.size)
          }

          val supportPerPartition = (numberRows/totalRows) * support
          val sliced = multipleSets.filter(elem => elem._2 >= supportPerPartition)

          sliced.toIterator
        })


        /////////////////////////////////////////////////////////////////////

        val filterMultiple = values.filter(elem => elem._2 >= support)
        forManySetCombinations = filterMultiple.map(elem => elem._1).collect()


        val fil = filterMultiple.collectAsMap().toSeq.sortWith(_._2 < _._2)

        //println("Here is the output: ")
        //fil.foreach(println)

        ///////////////////////////////////////////////print pair sets
        val sortingSetsMultiple = forManySetCombinations.sortBy(r => (r(0), r(1)))

        for (k <- sortingSetsMultiple) {
          val cutString = k.toString().replaceAll("Mutable", "")
          val cutString1 = cutString.replaceAll("List", "")
          writer.write(cutString1 + ", ")
        }
        writer.write("\n")
        writer.write("\n")
        ///////////////////////////////////////////////////////////////



        if (fil.size == 0) {
          isFind = false
        }

        combinationNumber += 1
      }

    }


    writer.close()

    val duration = (System.nanoTime - time) / 1e9d
    println("The time " + duration + " sec")
  }

}
