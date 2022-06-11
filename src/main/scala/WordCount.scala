import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Accumulator Demo")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val dataPath = "/Users/shaw.lu/Downloads/InstallGuideAndSourceCode/Datasets"
    val lines = sc.textFile(s"$dataPath/LogsExample/hbase.log")

    // flatMap merges all lines into a single array of words instead of array of arrays
    val words = lines.flatMap(_.split(" ")).map(x => (x, 1))
    val wordCounts = words.reduceByKey(_ + _).sortBy(-_._2)
    println("top 10 words:", wordCounts.take(10).mkString("Array(", ", ", ")"))
  }
}
