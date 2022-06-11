import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.joda.time.format._
import org.joda.time.LocalTime
import org.joda.time.LocalDate

object FlightAnalysis {
  case class Flight(date: LocalDate,
                    airline: String ,
                    flightnum: String,
                    origin: String ,
                    dest: String ,
                    dep: LocalTime,
                    dep_delay: Double,
                    arv: LocalTime,
                    arv_delay: Double ,
                    airtime: Double ,
                    distance: Double
                   )

  def parse(row: String): Flight={
    val fields = row.split(",")
    val datePattern = DateTimeFormat.forPattern("YYYY-mm-dd")
    val timePattern = DateTimeFormat.forPattern("HHmm")

    val date: LocalDate = datePattern.parseDateTime(fields(0)).toLocalDate()
    val airline: String = fields(1)
    val flightnum: String = fields(2)
    val origin: String = fields(3)
    val dest: String = fields(4)
    val dep: LocalTime = timePattern.parseDateTime(fields(5)).toLocalTime()
    val dep_delay: Double = fields(6).toDouble
    val arv: LocalTime = timePattern.parseDateTime(fields(7)).toLocalTime()
    val arv_delay: Double = fields(8).toDouble
    val airtime: Double = fields(9).toDouble
    val distance: Double = fields(10).toDouble

    Flight(date,airline,flightnum,origin,dest,dep,
      dep_delay,arv,arv_delay,airtime,distance)
  }

  def notHeader(row: String): Boolean = {
    !row.contains("Description")
  }

  def parseLookup(row: String): (String, String) = {
    // example row:
    // "01A","Afognak Lake, AK: Afognak Lake Airport"
    val x = row.replace("\"", "").split(",")
    (x(0), x(1))
  }

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Airline Analysis")
      .setMaster("local[2]")
      .set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    val dataPath = "/Users/shaw.lu/Downloads/InstallGuideAndSourceCode/Datasets/FlightsData"
    val airlinesPath = s"$dataPath/airlines.csv"
    val airportsPath = s"$dataPath/airports.csv"
    val flightsPath = s"$dataPath/flights.csv"

    val airlines = sc.textFile(airlinesPath)
    val airports = sc.textFile(airportsPath).filter(notHeader).map(parseLookup)
    val flights = sc.textFile(flightsPath)
    val flightsParsed = flights.map(parse)

    // average flight distance
    val totalDist = flightsParsed.map(_.distance).reduce((x, y) => x + y)
    val avgDist = totalDist / flightsParsed.count().toDouble
    println(s"avg flight dist $avgDist")

    // proportion of delayed flight
    val delayed = flightsParsed.filter(_.dep_delay > 0).count().toDouble
    val pctDelayed = delayed / flightsParsed.count().toDouble
    println(s"% of delayed flight: $pctDelayed")

    // average flight delay
    val count = flightsParsed.map(_.dep_delay).aggregate((0.0, 0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    val avgDelay = count._1 / count._2
    println(s"avg delay: $avgDelay")

    // build a histogram
    val dist = flightsParsed.map(x => (x.dep_delay / 60).toInt).countByValue()
    println("frequency distribution", dist)

    // Calculate average delay per airport, option 1: two pass
    val airportDelay = flightsParsed.map(x => (x.origin, x.dep_delay))
    println(airportDelay.keys.take(10).mkString("Array(", ", ", ")"))
    println(airportDelay.values.take(10).mkString("Array(", ", ", ")"))

    // get delay and flight count per airport
    val delay = airportDelay.reduceByKey((x, y) => x + y)
    val airportCount = airportDelay.mapValues(x => 1).reduceByKey((x, y) => x + y)
    val airportAvgDelay = delay.join(airportCount).mapValues(x => x._1 / x._2)
    println("airportAvgDelay", airportAvgDelay.sortBy(-_._2).take(10).mkString("Array(", ", ", ")"))

    // another way to calculate avg delay per airport
    val airportAvgDelay2 = airportDelay.combineByKey(
      value => (value, 1), // create combiner function
      (acc: (Double, Int), value) => (acc._1 + value, acc._2 + 1), // merge rdd
      (acc1: (Double, Int), acc2: (Double, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2) // merge accumulators after shuffle
    ).mapValues(x => x._1 / x._2)
    println("airportAvgDelay2", airportAvgDelay2.sortBy(-_._2).take(10).mkString("Array(", ", ", ")"))

    // lookup
    val airportKey = "PPG"
    val description = airports.lookup(airportKey)
    println(s"lookup airport $airportKey: $description")

    // collectAsMap is a special action for pair RDD, returns a map which is broadcast to all nodes
    val airportMap = airports.collectAsMap()
    val airportMapBC = sc.broadcast(airportMap)
    val airportAvgDelay3 = airportAvgDelay.map(x => (airportMapBC.value(x._1), x._2))
    println("airportAvgDelay with info", airportAvgDelay3.sortBy(-_._2).take(10).mkString("Array(", ", ", ")"))
    sc.stop()
  }
}