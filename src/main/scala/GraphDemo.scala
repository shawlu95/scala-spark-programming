import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}

object GraphDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Airline Analysis")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val dataPath = "/Users/shaw.lu/Downloads/InstallGuideAndSourceCode/Datasets"
    val edges = sc.textFile(s"$dataPath/MarvelData/Edges.csv")
      .map(_.split(","))
      .map(x => Edge(x(0).toLong, x(1).toLong, x(2).toInt))

    val vertices = sc.textFile(s"$dataPath/MarvelData/Vertices.csv")
      .map(_.split("[|]"))
      .map(x => (x(0).toLong, x.slice(1, x.length)))
      .distinct()

    val socialGraph = Graph(vertices, edges)

    // top 10 most frequent character
    // format after join: (4904,(136,Array(" SCATTERBRAIN", 11)))
    // x._1 is vertex id
    // x._2._1 is degree
    // x._2._2 is properties (character name, _)
    socialGraph.degrees
      .join(vertices)
      .map(x => (x._2._2(0).trim, x._2._1))
      .distinct()
      .sortBy(-_._2)
      .take(10)
  }
}
