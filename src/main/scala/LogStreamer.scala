import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object LogStreamer {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Stream Demo")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint("/Users/shaw.lu/Downloads/ssc")

    // pass in host and port when submitting to spark
    val lines = ssc.socketTextStream(args(0), args(1).toInt)

    // count words in error log
    val counts = lines
      .filter(_.contains("Error"))
      .flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_+_)
    counts.print()

    val windowedCount = lines
      .filter(_.contains("Error"))
      .flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKeyAndWindow(
        {(x, y) => x + y}, // rdd y moves into window
        {(x, y) => x - y}, // rdd y moves out of window
        Seconds(30), Seconds(10)) // window length and step
    windowedCount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
