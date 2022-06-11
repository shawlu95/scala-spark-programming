import org.apache.spark.sql.{SparkSession}

object DataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Accumulator Demo")
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val dataPath = "/Users/shaw.lu/Downloads/InstallGuideAndSourceCode/Datasets"
    val twitterTable = spark.read.json(s"$dataPath/twitter.json")
    twitterTable.createOrReplaceTempView("twitterTable")
    twitterTable.printSchema()

    val trumpTweets = spark.sql("select text, user.screen_name, entities " +
      "from twitterTable " +
      "where user.screen_name='realDonaldTrump' " +
      "limit 10")
    println(trumpTweets.collect().mkString("Array(", ", ", ")"))
  }
}
