import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object LogProcessor {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Accumulator Demo")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val dataPath = "/Users/shaw.lu/Downloads/InstallGuideAndSourceCode/Datasets/"
    val logsPath = s"$dataPath/LogsExample/hbase.log"

    val logs=sc.textFile(logsPath)
    logs.filter(_.contains("ERROR")).take(2)

    val errCount=sc.longAccumulator("errCount")

    def processLog(line: String): String={
      if (line.contains("ERROR")) {
        errCount.add(1)
      }
      line
    }

    // must trigger action to accumulate
    val logCount = logs.map(processLog).count()
    val accVal = errCount.value
    println(s"error count: $accVal / $logCount")
  }
}
