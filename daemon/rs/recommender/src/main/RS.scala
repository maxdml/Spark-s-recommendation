import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.{mutable,immutable}

import java.io._

object RS {
  def main(args: Array[String]) {
    val usage = "RS.scala <score_file>"
    if (args.length < 1) {
      println(usage)
      return
    }

    val conf = new SparkConf().setAppName("Recommendation system")
    val sc = new SparkContext(conf)

    // Load and parse the data
    val filename = if (args(0).length > 0) args(0) else return 
    val data = sc.textFile(filename)

    // Convert the score file to Rating(s) and cache them
    val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
        Rating(user.toInt, item.toInt, rate.toDouble)
      }).cache()
    

    // Build the recommendation model using ALS
    val rank = 50
    val numIterations = 13
    val model = ALS.train(ratings, rank, numIterations, 0.1)

    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))

    (ratings ++ predictions).collect().foreach(println)
  }
}
