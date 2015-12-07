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

    val conf = new SparkConf().setAppName("RS validation")
    val sc = new SparkContext(conf)

    // Load and parse the data
    val filename = if (args(0).length > 0) args(0) else return 
    val data = sc.textFile(filename)

    // Convert the score file to Rating(s) and cache them
    val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
        Rating(user.toInt, item.toInt, rate.toDouble)
      }).cache()


    val file =  "/tmp/AUC_test"
    appendToFile(file, "NEW VALIDATION RUN")

    // Split the data into a training and a test sets. Size will range from 10 to 90% sparsity.
    val sparsity = List[Double](0.1, 0.2, 0.2, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)
    //foreach degree of sparsity...
    val validation_map = sparsity.map { s =>
      println(s"Testing a sparsity degree of $s")
      val splits = ratings.randomSplit(Array(1-s, s))
      val training = splits(0).cache()
      val test = splits(1).cache()

      val training_count = training.count()
      val testing_count = test.count()
      appendToFile(file, s"Training set has ${training_count} elements")
      appendToFile(file, s"Testing set has ${testing_count} elements")
  
      // Build the recommendation model using ALS
      val rank = 50
      val numIterations = 13
      val model = ALS.train(training, rank, numIterations, 0.1)

      // Compute RMSE and mean of mean AUC for all users and product/users
      val validation: List[(String, Double)] = validateModel(model, test, training, ratings, false)
//      println(s"validation: rmse -> ${validation._1}, auc ->${validation._2}")
      List(s, validation)
    }

    validation_map.foreach(println)
  }

  def validateModel(model: MatrixFactorizationModel,
                    data: RDD[Rating],
                    training: RDD[Rating],
                    full_ratings: RDD[Rating],
                    implicitPrefs: Boolean): List[(String, Double)] = {

    def mapPredictedRating(r: Double): Double = {
      if (implicitPrefs) math.max(math.min(r, 1.0), 0.0) else r
    }

    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))

    // RMSE
    val predictionsAndRatings = predictions.map{ x =>
      ((x.user, x.product), mapPredictedRating(x.rating))
    }.join(data.map(x => ((x.user, x.product), x.rating)))
    
    val rmse = math.sqrt(predictionsAndRatings.values.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())

    // AUC
    //val file = new File("/tmp/AUC_test")
    val file =  "/tmp/AUC_test"
    
    // some ratings are dropped dunno why (in set: 333, expected: 336)
    //training_full: Array[(Int, List[org.apache.spark.mllib.recommendation.Rating])]
//    appendToFile(file, "full outcome data:")
    val full_pred = (training ++ predictions).groupBy(u => u.user).map{ case (user, list) =>
                      user -> list.toList.sortBy(rating => rating.rating)}.collect()
//    full_pred.map{case (u, l) => l.foreach{ l => appendToFile(file, l.toString) }}

//    appendToFile(file, "full original data:")
    val full_orig = full_ratings.groupBy(u => u.user).map{ case (user, list) =>
                      user -> list.toList.sortBy(rating => rating.rating)}.collect()
//    full_orig.map{case (u, l) => l.foreach{ l => appendToFile(file, l.toString) }}

    val size_training = (training ++ predictions).count()
    val size_test = full_ratings.count()
    appendToFile(file, s"training_size: $size_training, test_size: $size_test")
    //Rating(user_id, prod_id, score)
    //ranked_test: scala.collection.immutable.Iterable[List[Rating]] =
    //  List(List(Rating(10,9,1), Rating(10,9,5)), List(Rating(9,9,1), Rating(9,9,2)))
    //foreach user in the predictions
    var auc: Double = 0.0
    full_pred.foreach { case (user, list) => {
      var user_auc: Double =  0.0
      var product_auc: Double = 0.0
      //get the original application ranking list
      val user_prod_list = full_orig.filter(u => u._1 == user).head._2
      list.foreach { product =>
        //user filter should not be necessary but hey
        val real_prod = user_prod_list.filter( p => p.user == product.user && p.product == product.product).head
        val real_rank = user_prod_list.indexOf(real_prod)
        val pred_rank = list.indexOf(product)
        if (pred_rank > 0 && real_rank > 0) {
          var valid_ranks: Double = pred_rank.toDouble
          for (i <- 0 to pred_rank - 1) {
            val under_check = list(i)
            val real_under_check = user_prod_list.filter( p => p.user == under_check.user && p.product == under_check.product).head
            val uc_real_rank = user_prod_list.indexOf(real_under_check)
            // if above product was in reality worse than current product, and its value is not the same
            if (uc_real_rank > real_rank && under_check.rating != product.rating) {
              // decrement AUC for user's product
              valid_ranks = valid_ranks - 1.0
            }
          }
          product_auc = product_auc + (valid_ranks / pred_rank.toDouble)
        } else if (pred_rank == 0) {
          product_auc = product_auc + 1.0
        }
      }
      user_auc = user_auc + (product_auc / list.size.toDouble)
      auc = auc + user_auc
    }}
    auc = auc / full_pred.size

    appendToFile(file, s"mean_auc:  $auc, rmse: $rmse")
    appendToFile(file, "==============================================")
    appendToFile(file, "==============================================")

    List[(String, Double)](("rmse", rmse), ("auc", auc))

//    List[(String, Double)](("rmse", rmse), ("auc", 1.0))
  }

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B =
    try { f(param) } finally { param.close() }

  def appendToFile(fileName:String, textData:String) =
    using (new FileWriter(fileName, true)) {
      fileWriter => using (new PrintWriter(fileWriter)) {
        printWriter => printWriter.println(textData)
      }
  }
    // Save and load model
//    model.save(sc, "myModelPath")
}

    /*
    // Evaluate the model on rating data
    val reco = model.recommendProductsForUsers(112).collect()
    reco.foreach{ case (user, ratings) =>
      println("user: " + user)
      ratings.sortBy(r => r.rating).foreach(println)
    }
    */
