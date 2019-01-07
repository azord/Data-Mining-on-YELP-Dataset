import java.io.{File, PrintWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import collection.mutable

object Task1 {
  val time = System.nanoTime
  Logger.getLogger("org").setLevel(Level.FATAL)
  def main(args: Array[String]): Unit = {
    //val input = "C://Users/ordaz/OneDrive/Desktop/INF 553/HW/HW4/INF553_Assignment4/Data/yelp_reviews_clustering_small.txt"
    val input = args(0)
    //val feature = "T"
    val feature = args(1).toString
    //val clusterNumber = 5
    val clusterNumber = args(2).toInt
    //val iterations = 20
    val iterations = args(3).toInt
    //val outputPath = "C://Users/ordaz/OneDrive/Desktop/hw4/task1T.json"
    val outputPath = args(4)

    val spark_config = new SparkConf().setAppName("HW4").setMaster("local[*]")
    val sc = new SparkContext(spark_config)

    var str = ""


    val documents: RDD[Seq[String]] = sc.textFile(input).map(_.split(" ").toSeq)

    val writer = new PrintWriter(new File(outputPath))


    val hashingTF = new HashingTF()

    val listOfReviewsHashed = documents.map(x => (x, hashingTF.transform(x))).collectAsMap().toList


    //listOfReviewsHashed.foreach(println)


    val tf1: RDD[Vector] = sc.parallelize(listOfReviewsHashed.map(elem => elem._2).toVector)

    //var passData: RDD[Vector] = RDD[Vector]()

    if (feature == "T") {

      val idf = new IDF().fit(tf1)
      val tfidf: RDD[Vector] = idf.transform(tf1)


      val tfArray = tfidf.collect()
      var centroids = tfidf.takeSample(false, clusterNumber, 20181031)

      var clusterToReview = new mutable.MutableList[(Int, Vector)]()
      var counterIteration = 0

      while (counterIteration < iterations) {

        clusterToReview = new mutable.MutableList[(Int, Vector)]()
        val clusters = scala.collection.mutable.Map[Int, Vector]()
        var clusterID = 0

        //centroids.foreach(println)

        for (centroid <- centroids) {
          clusters(clusterID) = centroid
          clusterID = clusterID + 1
        }

        //clusters.foreach(println)

        //val clusterToReview = new mutable.MutableList[(Int, Vector)]()

        val clusters1 = scala.collection.mutable.Map[Int, List[Array[Double]]]()
        val errors = scala.collection.mutable.Map[Int, Double]()



        tfArray.foreach { elem =>
          var id = 0

          var mapValues = scala.collection.mutable.Map[Int, Double]()
          clusters.foreach { e =>
            val distance = Vectors.sqdist(e._2, elem)
            mapValues(e._1) = distance
          }
          var lowestDistanceId = mapValues.minBy(_._2)._1
          var lowestDistance = mapValues.minBy(_._2)._2

          if (errors.contains(lowestDistanceId)) {
            var oldValue = errors(lowestDistanceId) + lowestDistance
            errors(lowestDistanceId) = oldValue
          } else {
            errors(lowestDistanceId) = lowestDistance
          }

          clusterToReview += ((lowestDistanceId, elem))

        }



        val vectorRdd = sc.parallelize(clusterToReview)

        val reduced = vectorRdd.reduceByKey { case (a: (Vector), b: (Vector)) =>
          Vectors.dense((a.toArray, b.toArray).zipped.map(_ + _))
        }


        val numberOfOccurences = clusterToReview.groupBy(_._1).mapValues(_.size)


        val sumOfVectors = reduced.collectAsMap()

        val divided = sumOfVectors.map { case elem =>
          val size = numberOfOccurences(elem._1)
          val r = elem._2.toArray.map(_ / size)

          Vectors.dense(r)
        }



        centroids = divided.toArray

        counterIteration += 1



        if (counterIteration == iterations - 1) {


          numberOfOccurences.foreach{elem =>
            /*println(errors(elem._1))
            println("Number of elements: ")
            println(elem._1 + " " + elem._2)*/
          }

          var sumOfErrors = 0.0

          errors.foreach{elem =>
            sumOfErrors += elem._2
          }

          str += ("{")
          str += ("\n")
          str += ("\"algorithm\": \"K-Means\",")
          str += ("\n")
          str += ("\"WSSE\": " + sumOfErrors + ",")
          str += ("\n")
          str += ("\"clusters\": [ ")


          var reviewToCluster = for ( (m, a) <- (clusterToReview zip listOfReviewsHashed)) yield (a._1, m._1)


          var clusterID = 0
          while (clusterID  < clusterNumber) {
            val filteredDataOriginal = reviewToCluster.filter(elem => elem._2 == clusterID).map(elem => elem._1.toString()).toString()

            val sliced = filteredDataOriginal.slice(18, filteredDataOriginal.length).stripSuffix(",")


            val ssr = sliced.split(" ").groupBy(identity).map(elem => (elem._1, elem._2.size)).toSeq.sortWith(_._2 > _._2).take(10)

            //println("ClusterId: " + clusterID)
            //ssr.foreach(print)
            //println()




            str += ("\n")
            str += (" { ")
            str += ("\n")
            str += ("\"id\": " + clusterID + "," )
            str += ("\n")
            str += ("\"size\":" + numberOfOccurences(clusterID) + ",")
            str += ("\n")
            str += ("\"error\":" + errors(clusterID) + ",")
            str += ("\n")
            str += ("\"terms\": [")

            ssr.foreach{elem =>
              str += ("\"" + elem._1.replaceAll(",", "") + "\", ")
            }
            str += ("]")
            str += ("\n" + "},")

            clusterID += 1
          }



        }
      }














    }
    else {
      val tfArray = tf1.collect()
      var centroids = tf1.takeSample(false, clusterNumber, 20181031)

      var clusterToReview = new mutable.MutableList[(Int, Vector)]()
      var counterIteration = 0

      while (counterIteration < iterations) {

        clusterToReview = new mutable.MutableList[(Int, Vector)]()
        val clusters = scala.collection.mutable.Map[Int, Vector]()
        var clusterID = 0

        //centroids.foreach(println)

        for (centroid <- centroids) {
          clusters(clusterID) = centroid
          clusterID = clusterID + 1
        }

        //clusters.foreach(println)

        //val clusterToReview = new mutable.MutableList[(Int, Vector)]()

        val clusters1 = scala.collection.mutable.Map[Int, List[Array[Double]]]()
        val errors = scala.collection.mutable.Map[Int, Double]()



        tfArray.foreach { elem =>
          var id = 0

          var mapValues = scala.collection.mutable.Map[Int, Double]()
          clusters.foreach { e =>
            val distance = Vectors.sqdist(e._2, elem)
            mapValues(e._1) = distance
          }
          var lowestDistanceId = mapValues.minBy(_._2)._1
          var lowestDistance = mapValues.minBy(_._2)._2

          if (errors.contains(lowestDistanceId)) {
            var oldValue = errors(lowestDistanceId) + lowestDistance
            errors(lowestDistanceId) = oldValue
          } else {
            errors(lowestDistanceId) = lowestDistance
          }

          clusterToReview += ((lowestDistanceId, elem))

        }



        val vectorRdd = sc.parallelize(clusterToReview)

        val reduced = vectorRdd.reduceByKey { case (a: (Vector), b: (Vector)) =>
          Vectors.dense((a.toArray, b.toArray).zipped.map(_ + _))
        }


        val numberOfOccurences = clusterToReview.groupBy(_._1).mapValues(_.size)


        val sumOfVectors = reduced.collectAsMap()

        val divided = sumOfVectors.map { case elem =>
          val size = numberOfOccurences(elem._1)
          val r = elem._2.toArray.map(_ / size)

          Vectors.dense(r)
        }



        centroids = divided.toArray

        counterIteration += 1



        if (counterIteration == iterations - 1) {
          numberOfOccurences.foreach{elem =>
            println(errors(elem._1))
            println("Number of elements: ")
            println(elem._1 + " " + elem._2)
          }

          var sumOfErrors = 0.0

          errors.foreach{elem =>
            sumOfErrors += elem._2
          }

          str += ("{")
          str += ("\n")
          str += ("\"algorithm\": \"K-Means\",")
          str += ("\n")
          str += ("\"WSSE\": " + sumOfErrors + ",")
          str += ("\n")
          str += ("\"clusters\": [ ")


          var reviewToCluster = for ( (m, a) <- (clusterToReview zip listOfReviewsHashed)) yield (a._1, m._1)


          var clusterID = 0
          while (clusterID  < clusterNumber) {
            val filteredDataOriginal = reviewToCluster.filter(elem => elem._2 == clusterID).map(elem => elem._1.toString()).toString()

            val sliced = filteredDataOriginal.slice(18, filteredDataOriginal.length).stripSuffix(",")


            val ssr = sliced.split(" ").groupBy(identity).map(elem => (elem._1, elem._2.size)).toSeq.sortWith(_._2 > _._2).take(10)

            println("ClusterId: " + clusterID)
            ssr.foreach(print)
            println()


            str += ("\n")
            str += (" { ")
            str += ("\n")
            str += ("\"id\": " + clusterID + "," )
            str += ("\n")
            str += ("\"size\":" + numberOfOccurences(clusterID) + ",")
            str += ("\n")
            str += ("\"error\":" + errors(clusterID) + ",")
            str += ("\n")
            str += ("\"terms\": [")

            ssr.foreach{elem =>
              str += ("\"" + elem._1.replaceAll(",", "") + "\", ")
            }
            str += ("]")
            str += ("\n" + "},")



            clusterID += 1
          }



        }
      }


    }

    //////////////////////////////////////////////////////////////////////////////////////////////////


    writer.write(str)
    writer.close()
  }


  /*def distance(xs: Vector[Int], ys: Vector[Int]) = {
    sqrt((xs zip ys).map { case (x,y) => pow(y - x, 2) }.sum)
  }*/
}

