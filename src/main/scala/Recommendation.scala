import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

object Recommendation {
  case class Rating(userId: Int, artistId: Int, rating: Int)
  def parseRating(str: String): Rating = {
    val fields = str.split(" ")
    assert(fields.size == 3)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toInt)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkSQL Demo")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext
    val dataPath = "/Users/shaw.lu/Downloads/InstallGuideAndSourceCode/Datasets"
    val data = sc
      .textFile(s"$dataPath/AudioscrobblerData/user_artist_data_small.txt")
      .map(parseRating)

    val stats = data.map(_.rating).stats()
    println("rating stats", stats)

    val uaData = data.filter(_.rating >= 20)
    val df = spark.createDataFrame(uaData)

    val Array(training, test) = df.randomSplit(Array(0.8, 0.2))

    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("artistId")
      .setRatingCol("rating")
    val model = als.fit(training)

    // Note we set cold start strategy to 'drop' to
    // ensure we don't get NaN evaluation metrics
    model.setColdStartStrategy("drop")
    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")

    // Generate top 10 music recommendations for each user
    val userRecs = model.recommendForAllUsers(10)
    println(s"userRecs", userRecs.collect().mkString("Array(", ", ", ")"))

    // Generate top 10 user recommendations for each music
    val musicRecs = model.recommendForAllItems(10)
    println(s"musicRecs", musicRecs.collect().mkString("Array(", ", ", ")"))
  }
}
