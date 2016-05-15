package subway.ContrastExperiment

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import subway.passengerTripListAddFeatures
import scala.collection.mutable.ArrayBuffer

/**
 * 对比试验，走过次数最多的路线
 * Created by Flyln on 16/5/6.
 */
object theMostVisitedLine {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("mostVisited")
    val sc = new SparkContext(conf)

    val data = sc.textFile("/Users/Flyln/Desktop/predictData/sample1101")

    val result: RDD[Double] = data.map(_.split(",")).map(x => passengerTripListAddFeatures(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8))).map(x => {
      val tripArray = new ArrayBuffer[String]
      val predictLine = new ArrayBuffer[String]
      val trip: Array[String] = x.tripList.split("->")
      for (i <- (trip.length - 6) to (trip.length - 2)) {
        for (j <- 0 to i) tripArray += trip(j)
        val distinctTripArray = tripArray.distinct
        val timeArray = new Array[Int](distinctTripArray.length)
        for (i <- distinctTripArray.indices) {
          for (j <- tripArray.indices) {
            if (distinctTripArray(i) == tripArray(j)) timeArray(i) += 1
          }
        }
        var max = timeArray(0)
        var k = 0
        for (i <- timeArray.indices) {
          if (timeArray(i) > max) {
            max = timeArray(i)
            k = i
          }
        }
        predictLine += distinctTripArray(k)
      }
      accuracyLine(predictLine, trip)
    })

    result.saveAsTextFile("")

  }

  def accuracyLine(predictLine: ArrayBuffer[String], trip: Array[String]):Double = {
    val lastFiveTrip = new ArrayBuffer[String]
    for (i <- (trip.length - 5) to (trip.length - 1)) lastFiveTrip += trip(i)
    var k = 0
    for (i <- predictLine.indices) {
      if (predictLine(i) == lastFiveTrip(i)) k += 1
    }
    k/5.0
  }

}
