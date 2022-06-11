import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object PageRank {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Accumulator Demo")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val dataPath = "/Users/shaw.lu/Downloads/InstallGuideAndSourceCode/Datasets"
    val data = sc.textFile(s"$dataPath/PageRank/web-Google.txt")
      .filter(!_.contains("#"))
      .map(_.split("\t"))
      .map(x => (x(0), x(1)))

    // create a links RDD with all outgoing links from each page
    // this is used for every iteration, cache improves performance
    val links = data.groupByKey().cache()

    var ranks = links.mapValues(v => 1.0)
    for (i <- 1 to 10) {
      // transfer rank to all destination pages
      val contribs = links.join(ranks).values.flatMap { case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }
    println("top 10 pages", ranks.sortBy(-_._2).take(10).mkString("Array(", ", ", ")"))
  }
}
