package subway.ContrastExperiment

import scala.collection.mutable.ArrayBuffer
import subway.delegateFunctions.constructSecondOrderMarKovMatrix
import subway.delegateFunctions.chooseFromSecondMatrix
import subway.delegateFunctions.constructMarkovMatrix
import subway.delegateFunctions.chooseFromMatrix
/**
 * 对比试验，马尔科夫链
 * Created by Flyln on 16/5/14.
 */
object marKovChain {

  def oneOrderMarKov(trip: ArrayBuffer[String]):String = {
    val matrix = constructMarkovMatrix(trip)
    if (trip.distinct.length == 1) trip.last
    else chooseFromMatrix(matrix, trip)(2)
  }

  def twoOrderMarKov(trip: ArrayBuffer[String]):String = {
    val matrix = constructSecondOrderMarKovMatrix(trip)
    if (trip.distinct.length == 1) trip.last
    else chooseFromSecondMatrix(matrix, trip)
  }
}
