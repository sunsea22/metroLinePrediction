package subway

import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.{ListBuffer, ArrayBuffer}

/**地铁线路预测算法
 * Created by Flyln on 16/5/4.
 */
object predictionAlgorithm {
  var a1One = 0
  var a1Two = 0
  var a2First = 0
  var a2One = 0
  var a2Two = 0
  var a2Three = 0
  var a2Four = 0
  var a5One = 0
  var a5Two = 0
  var a3 = 0

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[6]").setAppName("prediction")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("/Users/Flyln/Desktop/predictData/moreThan100/sampleChangeResidence")
    val metroLine = sc.textFile("/Users/Flyln/Desktop/predictData/metroLine")
    val trainingData = sc.textFile("/Users/Flyln/Desktop/predictData/moreThan100/trainingData")
    val transferLine = sc.textFile("/Users/Flyln/Desktop/predictData/moreThan100/transferLine")
    val cluster = sc.textFile("/Users/Flyln/Desktop/predictData/cluster/cardId-ClusterId-tripList")


    val transferLineArray: Array[String] = transferLine.collect()
    val metroLineArray = metroLine.collect()
    val clusterResult = cluster.collect()

    val clusterArray = cluster.map(_.split(",")).map(x => {
      val tripList = new ArrayBuffer[(String,String)]
      val trip = x(2).split("->").slice(0, x(2).split("->").length - 5)
      for (i <- trip.indices) tripList += ((x(1),trip(i)))
      tripList.map((_,1))
    }).flatMap(x => x).reduceByKey(_+_).collect()

    val attArray: Array[(String, Int)] = rdd.map(_.split(",")).map(r => passengerTripListAddFeatures(r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8))).map(x => {
      val tripArray = x.tripList.split("->")
      val tmpArray = new ArrayBuffer[String]
      for (i <- x.tripList.split("->").indices) {
        tmpArray += tripArray(i)
      }
      tmpArray.map((_, 1))
    }).flatMap(x => x).reduceByKey(_ + _).collect()

    val weekAndTimeLine: Array[(String, Int)] = rdd.map(_.split(",")).map(x => x(4) + "," + x(5) + "," + x(6)).map(_.split(",")).map(x => {
      val tmpArray = new ArrayBuffer[String]
      for (i <- 0 to (x(0).split("=>").length - 6)) tmpArray += x(0).split("=>")(i) + "->" + x(1).split("=>")(i) + "=>" + x(2).split("->")(i)
      tmpArray.map((_,1))
    }).flatMap(x => x).reduceByKey(_+_).collect()

    val data = trainingData.map(_.split(",")).map(x => x(7) + "," + x(8))
    val parsedData = data.map(line => {
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(_.toDouble)))
    })
    val bayesNewOrHistory: NaiveBayesModel = NaiveBayes.train(parsedData, lambda = 1.0, modelType = "multinomial")

    val result = startUp(rdd,metroLineArray,bayesNewOrHistory,attArray,transferLineArray,weekAndTimeLine,clusterArray,clusterResult)

      result.repartition(1).saveAsTextFile("/Users/Flyln/Desktop/predictData/moreThan100/result")
    println(a1One)
    println(a1Two)
    println(a2First)
    println(a2One)
    println(a2Two)
    println(a2Three)
    println(a2Four)
    println(a5One)
    println(a5Two)
    println(a3)
  }

  def startUp(rdd: RDD[String],metroLineArray: Array[String],bayesNewOrHistory: NaiveBayesModel,attArray:Array[(String,Int)],transferLineArray: Array[String],weekAndTimeLine:Array[(String,Int)],clusterArray: Array[((String,String),Int)],clusterResult:Array[String]): RDD[String] = {
    val decision = rdd.map(_.split(",")).map(x => passengerTripListAddFeatures(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8))).map(x => {
      val trip = x.tripList.split("->")
      val weekNum = x.weekNum.split("=>")
      val timeNum = x.departureTime.split("=>")
      val departure = x.departureStation.split("->")
      val arrive = x.arriveStation.split("->")
      val tmpArray = new ArrayBuffer[String]()
      val weekNumArray = new ArrayBuffer[Int]()
      val timeNumArray = new ArrayBuffer[Int]()
      val departureArray = new ArrayBuffer[String]
      val arriveArray = new ArrayBuffer[String]
      val predictionLine = new ArrayBuffer[String]
      for (i <- (trip.size - 6) to (trip.size - 2)) {
        for (j <- 0 to i) {
          tmpArray += trip(j)
          weekNumArray += weekNum(j).toInt
          timeNumArray += timeNum(j).toInt
          departureArray += departure(j)
          arriveArray += arrive(j)
        }
        if (tmpArray.distinct.size == 1) predictionLine += tmpArray.last
        else if (x.fourFeatures.split(" ")(2).toDouble < 5.0) predictionLine += "A5" + "->" + algorithmFive(x.cardId,tmpArray,transferLineArray,metroLineArray,departure(i),arrive(i),attArray,x.residence)
        else if (departure(i) == x.residence) predictionLine += "A1" + "->" + algorithmOne(tmpArray, metroLineArray, x.residence,departure(i),arrive(i))
        else if (arrive(i) != x.residence) predictionLine += "A2" + "->" + algorithmTwo(tmpArray, metroLineArray, x.residence,weekNumArray,timeNumArray,departureArray,arriveArray)
        else if (bayesNewOrHistory.predict(bayesDataFormat(x.labelNew + "," + x.fourFeatures).features) == 1.0) predictionLine += "A3" + "->" + algorithmThree(tmpArray,metroLineArray,departure(i),arrive(i),x.residence)
        else predictionLine += "A4" + "->" + algorithmFour(tmpArray, weekNumArray, timeNumArray)
      }
      x.cardId + "," + calculateAccuracy(predictionLine, trip)
    })
    decision
  }

  def calculateAccuracy(predictionLine: ArrayBuffer[String], AllTrip: Array[String]): String = {
    var k = 0
    val trip = predictionLine.map(_.split("->")(1))
    val algorithm = predictionLine.map(_.split("->")(0))
    val lastFiveTrip = new ArrayBuffer[String]
    for (i <- (AllTrip.length - 5) to (AllTrip.length - 1)) lastFiveTrip += AllTrip(i)
    for (i <- trip.indices) {
      if (trip(i).contains("Y")) {
        if (trip(i).init == lastFiveTrip(i)) {
          k += 1
          algorithm(i) += "TY"
        }
        else algorithm(i) += "FY"
      }
        else if (trip(i).contains("V")) {
        if (trip(i).init == lastFiveTrip(i)) {
          k += 1
          algorithm(i) += "TV"
        }
        else algorithm(i) += "FV"
      }
      else if (trip(i).contains("S")) {
        if (trip(i).init == lastFiveTrip(i)) {
          k += 1
          algorithm(i) += "TS"
        }
        else algorithm(i) += "FS"
      }
      else if (trip(i).contains("R")) {
        if (trip(i).init == lastFiveTrip(i)) {
          k += 1
          algorithm(i) += "TR"
        }
        else algorithm(i) += "FR"
      }
      else {
        if (trip(i) == lastFiveTrip(i)) {
          k += 1
          algorithm(i) += "T"
        }
        else algorithm(i) += "F"
      }
    }
    AllTrip.length + "," + algorithm.mkString("->") + "," + k / 5.0
  }

  /**
   * 构建MarKov状态转移矩阵
   * @param tripList 历史路线
   * @return
   */
  def constructMarkovMatrix(tripList: ArrayBuffer[String]): Array[Array[Int]] = {
    val tmpList = new ListBuffer[String]()
    for (i <- tripList.indices) {
      tmpList += tripList(i)
    }
    val tmp = tmpList.distinct
    val transferMatrix = Array.ofDim[Int](tmp.size, tmp.size)
    var m = 0
    var n = 0
    for (i <- 0 to (tripList.size - 2)) {
      for (k <- tmp.indices) {
        if (tripList(i) == tmp(k)) m = k
      }
      for (j <- tmp.indices) {
        if (tripList(i + 1) == tmp(j)) n = j
      }
      transferMatrix(m)(n) += 1
    }
    transferMatrix
  }


  /**
   * 从状态转移矩阵中选取两条跟当前路线相关，次数最多的路线
   * @param matrix 状态转移矩阵
   * @param trip 历史路线
   * @return
   */
  def chooseFromMatrix(matrix: Array[Array[Int]], trip: ArrayBuffer[String]): ArrayBuffer[String] = {
    val result = new ArrayBuffer[String]
    val tmpList = new ListBuffer[String]
    var tmpRow = 0
    var tmpClu = 0
    var secondTmpClu = 0
    val timeList = new ListBuffer[Int]
    val currentLine = trip.last
    for (i <- trip.indices) {
      tmpList += trip(i)
    }
    val uniqueTmp = tmpList.distinct
    for (i <- uniqueTmp.indices) {
      if (currentLine == uniqueTmp(i)) tmpRow = i
    }
    for (j <- matrix.indices) {
      timeList += matrix(tmpRow)(j)
    }
    val sortList = timeList.sorted
    val a = sortList.last
    val b = sortList.init.last
    for (i <- timeList.indices) {
      if (timeList(i) == a) tmpClu = i
    }
    for (i <- timeList.indices) {
      if (timeList(i) == b) secondTmpClu = i
    }
    result += a.toString
    result += b.toString
    result += uniqueTmp(tmpClu)
    result += uniqueTmp(secondTmpClu)
  }


  /**
   * 从状态转移矩阵中找到次数累加值最大的列
   * @param matrix 状态转移矩阵
   * @param trip 历史路线
   * @return 返回对应列的轨迹
   */
  def chooseOneLineFromMatrix(matrix: Array[Array[Int]], trip: ArrayBuffer[String]): String = {
    val tmpList = new ListBuffer[String]
    val sumArray = new ArrayBuffer[Int]()
    var tmpSum = 0
    for (i <- trip.indices) {
      tmpList += trip(i)
    }
    tmpList.distinct
    for (j <- matrix.indices) {
      for (i <- matrix.indices) {
        tmpSum += matrix(i)(j)
      }
      sumArray += tmpSum
    }
    var max = sumArray.head
    var row = 0
    for (i <- 1 to (sumArray.size - 1)) {
      if (sumArray(i) > max) {
        max = sumArray(i)
        row = i
      }
    }
    tmpList(row)
  }

  /**
   * 算法1 当前的出发站台等于常住地时
   * @param trip 历史轨迹，包括当前的轨迹
   * @param metroLineArray 各个线路的标识
   * @param home 常住地
   * @param arrive 当前的到达
   * @return
   */
  def algorithmOne(trip: ArrayBuffer[String], metroLineArray: Array[String], home: String,departure:String,arrive: String): String = {
    val matrixArray = new ArrayBuffer[String]()
    var line = ""
    var line1= ""
    for (i <- metroLineArray.indices) {
      if (arrive == metroLineArray(i).split(",")(0) && home == metroLineArray(i).split(",")(1)) line = metroLineArray(i).split(",")(2)
    }
    for (i <- metroLineArray.indices) {
      if (arrive == metroLineArray(i).split(",")(0) && departure == metroLineArray(i).split(",")(1)) line1 = metroLineArray(i).split(",")(2)
    }
    val threshold = 2
    for (i <- trip.indices) {
      matrixArray += trip(i)
    }
    val matrix = constructMarkovMatrix(matrixArray)
    val maxTimeArray = chooseFromMatrix(matrix,trip)
    if (maxTimeArray.head.toInt - maxTimeArray(1).toInt > threshold) {
      a1One += 1
      maxTimeArray(2)
    }
    else {
      a1Two += 1
      line + "Y"
    }
  }


  /**
   * 算法2 当前的出发地与目的地均不为常住地
   * @param trip 历史轨迹，最后一条为当前轨迹
   * @param metroLineArray 各线路标识
   * @param home 常住地
   * @return
   */
  def algorithmTwo(trip: ArrayBuffer[String],metroLineArray: Array[String], home: String,weekNum:ArrayBuffer[Int],timeNum:ArrayBuffer[Int],departureArray:ArrayBuffer[String],arriveArray: ArrayBuffer[String]): String = {
    val matrixArray = new ArrayBuffer[String]()
    var chooseLineDeparture = ""
    var chooseLineArrive = ""
    var line = ""
    var line1 = ""
    for (i <- metroLineArray.indices) {
      if (arriveArray.last == metroLineArray(i).split(",")(0) && home == metroLineArray(i).split(",")(1)) line = metroLineArray(i).split(",")(2)
    }
    for (i <- metroLineArray.indices) {
      if (arriveArray.last == metroLineArray(i).split(",")(0) && departureArray.last == metroLineArray(i).split(",")(1)) line1 = metroLineArray(i).split(",")(2)
    }
    val threshold = 2
    for (i <- trip.indices) {
      matrixArray += trip(i)
    }
    val matrix = constructMarkovMatrix(matrixArray)
    val maxTimeArray = chooseFromMatrix(matrix, trip)
    for (i <- metroLineArray.indices) {
      if (chooseOneLineFromMatrix(matrix, trip) == metroLineArray(i).split(",")(2)) {
        chooseLineDeparture = metroLineArray(i).split(",")(0)
        chooseLineArrive = metroLineArray(i).split(",")(1)
      }
    }
    if (maxTimeArray.head.toInt != 0 && maxTimeArray(1).toInt == 0) {
      a2First += 1
      maxTimeArray(2) + "R"
    }
    else if (maxTimeArray.head.toInt - maxTimeArray(1).toInt > threshold) {
      a2One += 1
      maxTimeArray(2)
    }
    else if (chooseLineDeparture == arriveArray.last && chooseLineArrive == home) {
      a2Two += 1
      chooseOneLineFromMatrix(matrix, trip)
    }
    else if (chooseLineDeparture == arriveArray.last && chooseLineArrive == departureArray.last) {
      a2Three +=1
      chooseOneLineFromMatrix(matrix, trip)
    }
    else {
      a2Four += 1
      line + "Y"
    }

  }


//  /**
//   * 加入换乘，时间，吸引力因素
//   * @param trip 历史轨迹
//   * @param weekNum 出发星期
//   * @param timeNum 出发时间
//   * @param transferLineArray 换乘信息
//   * @param weekAndTimeLine 时间信息
//   * @return
//   */
//  def transferAndWeekAndTimeAndAtt(trip: ArrayBuffer[String],weekNum:ArrayBuffer[Int],timeNum:ArrayBuffer[Int],transferLineArray:Array[String],weekAndTimeLine:Array[(String,Int)]):String = {
//    val currentState = (weekNum.last,timeNum.last)
//    var theNextCurrent = ""
//    if (currentState._2 == 0) theNextCurrent = weekNum.last + "->" + 1
//    else if (currentState._2 == 1) theNextCurrent = weekNum.last + "->" + 2
//    else if(currentState._2 == 2 && currentState._1 < 6) theNextCurrent = weekNum.last+1 + "->" + 0
//    else theNextCurrent = 0 + "->" + 0
//    val weekAndTimeArray = new ArrayBuffer[(String,Int)]
//    for (i <- weekAndTimeLine.indices) {
//      if (theNextCurrent == weekAndTimeLine(i)._1.split("=>")(0)) weekAndTimeArray += weekAndTimeLine(i)
//    }
//    val theCurrentTrip = trip.last
//    var k = ""
//    for (i <- transferLineArray.indices) {
//      if (transferLineArray(i).split(",")(0) == theCurrentTrip) k = transferLineArray(i).split(",")(1)
//    }
//    val transferArray = new ArrayBuffer[String]
//    for (i <- transferLineArray.indices) {
//      if (transferLineArray(i).split(",")(1) == k) transferArray += transferLineArray(i).split(",")(0)
//    }
//    val transferAndWeek = new ArrayBuffer[(String,Int)]
//    for (i <- transferArray.indices) {
//      for (j <- weekAndTimeArray.indices) {
//        if (transferArray(i) == weekAndTimeArray(j)._1.split("=>")(1)) transferAndWeek += weekAndTimeArray(j)
//      }
//    }
//    transferAndWeek.sortBy(_._2).last._1.split("=>")(1)
//  }

//  /**
//   * 根据当前的路线，从换乘矩阵中找出与当前路线换乘一样的路线，根据该路线再从路线吸引力矩阵中找出出去历史路线的吸引力最大的路线
//   * @param trip 历史轨迹
//   * @param transferLineArray 换乘矩阵
//   * @param attArray 吸引力矩阵
//   * @return
//   */
//  def transferLineCurrentTrip(trip: ArrayBuffer[String], transferLineArray: Array[String], attArray: Array[(String,Int)]):String = {
//    val theCurrentTrip = trip.last
//    var k = ""
//    for (i <- transferLineArray.indices) {
//      if (transferLineArray(i).split(",")(0) == theCurrentTrip) k = transferLineArray(i).split(",")(1)
//    }
//    val tripArray = new ArrayBuffer[String]
//    for (i <- transferLineArray.indices) {
//      if (transferLineArray(i).split(",")(1) == k) tripArray += transferLineArray(i).split(",")(0)
//    }
//    val attAndTransfer = new ArrayBuffer[(String,Int)]
//    for (i <- tripArray.indices) {
//      for (j <- attArray.indices) {
//        if (tripArray(i) == attArray(j)._1) attAndTransfer += attArray(j)
//      }
//    }
//    attAndTransfer.sortBy(_._2).last._1
//  }
//  def theMostCurrentArriveFromWeekAndTimeMatrix(trip: ArrayBuffer[String],weekNum:ArrayBuffer[Int],timeNum:ArrayBuffer[Int],metroLineArray:Array[String],arrive:String):String = {
//    val tmpArray = new ArrayBuffer[Int]
//    val arriveArray = new ArrayBuffer[String]
//    val weekAndTimeArray = constructWeekAndTimeMatrix(trip,weekNum,timeNum)
//    val timeArray = new ArrayBuffer[Int]
//    val maxArray = new ArrayBuffer[Int]
//    for (i <- weekAndTimeArray.indices) tmpArray += weekAndTimeArray(i)(0)
//    for (i <- metroLineArray.indices) {
//      if (arrive == metroLineArray(i).split(",")(1)) arriveArray += metroLineArray(i).split(",")(2)
//    }
//    for (i <- tmpArray.indices) {
//      for (j <- arriveArray.indices) {
//        if (tmpArray(i).toString == arriveArray(j)) timeArray += tmpArray(i)
//      }
//    }
//    if (timeArray.isEmpty) trip.init.last
//    else {
//      for (i <- timeArray.indices) {
//        for (j <- tmpArray.indices) {
//          if (timeArray(i) == tmpArray(j)) maxArray += j
//        }
//      }
//      val tmp = new Array[Int](maxArray.length)
//      for (i <- maxArray.indices) tmp(i) = weekAndTimeArray(maxArray(i))(1)
//      tmpArray(maxArray(maxForArray(tmp))).toString
//    }
//  }
//
//
  /**
   * 当前路线是否为新的路线
   * @param trip 历史轨迹，最后一条为当前路线
   * @return
   */
  def tripNew(trip: ArrayBuffer[String]):Boolean = {
    val tmpArray = new ArrayBuffer[String]
    for (i <- 0 to (trip.length - 2)) tmpArray += trip(i)
    val lastTrip = trip.last
    var k = 0
    val distinctArray: ArrayBuffer[String] = tmpArray.distinct
    for (i <- distinctArray.indices) {
      if (lastTrip == distinctArray(i)) k += 1
    }
    if (k > 0) false
    else true
  }
//
//
//  /**
//   * 最多到达的车站
//   * @param arrive 历史到达车站集合
//   * @return
//   */
//  def mostArrive(arrive:ArrayBuffer[String]):String = {
//    val distinctArrive = arrive.distinct
//    val tmpArray = new Array[Int](distinctArrive.length)
//    for (i <- distinctArrive.indices) {
//      for (j <- arrive.indices) {
//        if (distinctArrive(i) == arrive(j)) tmpArray(i) += 1
//      }
//    }
//    distinctArrive(maxForArray(tmpArray))
//  }
//
//  /**
//   * 最多离开车站
//   * @param departure 历史离开车站集合
//   * @return
//   */
//  def mostDeparture(departure: ArrayBuffer[String]):String = {
//    val distinctDeparture = departure.distinct
//    val tmpArray = new Array[Int](distinctDeparture.length)
//    for (i <- distinctDeparture.indices) {
//      for (j <- departure.indices) {
//        if (distinctDeparture(i) == departure(j)) tmpArray(i) += 1
//      }
//    }
//    distinctDeparture(maxForArray(tmpArray))
//  }

//  /**
//   * 周末走过最多的路线
//   * @param trip 历史轨迹
//   * @param weekNum 星期
//   * @param timeNum 出发时间
//   * @return
//   */
//  def mostVisitedForWeek(trip: ArrayBuffer[String], weekNum: ArrayBuffer[Int], timeNum: ArrayBuffer[Int]):String = {
//    val weekTrip = new ArrayBuffer[String]
////    val morningWeekTrip = new ArrayBuffer[String]
////    val afternoonWeekTrip = new ArrayBuffer[String]
////    val eveningWeekTrip = new ArrayBuffer[String]
//    for (i <- trip.indices) {
//      if (weekNum(i) == 5 || weekNum(i) == 6) weekTrip += trip(i)
//    }
////    for (i <- trip.indices) {
////      if ((weekNum(i) == 5 || weekNum(i) == 6)&&timeNum(i) == 0) morningWeekTrip += trip(i)
////    }
////    for (i <- trip.indices) {
////      if ((weekNum(i) == 5 || weekNum(i) == 6)&&timeNum(i) == 1) afternoonWeekTrip += trip(i)
////    }
////    for (i <- trip.indices) {
////      if ((weekNum(i) == 5 || weekNum(i) == 6)&&timeNum(i) == 2) eveningWeekTrip += trip(i)
////    }
//    maxTimeForStringArray(trip)
//  }

//  /**
//   * 返回数组中出现次数最多的元素
//   * @param tmpArray 数组
//   * @return
//   */
//  def maxTimeForStringArray(tmpArray: ArrayBuffer[String]):String = {
//    val distinctArray = tmpArray.distinct
//    val timeArray = new Array[Int](distinctArray.length)
//    for (i <- distinctArray.indices) {
//      for (j <- tmpArray.indices) {
//        if (distinctArray(i) == tmpArray(j)) timeArray(i) += 1
//      }
//    }
//    distinctArray(maxForArray(timeArray))
//  }



//  /**
//   * 从所有线路中找出返回常住地的所有线路，并选出访问次数最多的线路
//   * @param trip 历史路线
//   * @param metroLineArray 所有线路集合
//   * @param home 常住地
//   * @return
//   */
//  def historyBackHome(trip: ArrayBuffer[String],metroLineArray: Array[String], home: String):String = {
//    val homeArray = new ArrayBuffer[String]
//    val departureArray = new ArrayBuffer[String]
//    for (i <- metroLineArray.indices) {
//      if (metroLineArray(i).split(",")(1) == home) homeArray += metroLineArray(i).split(",")(2)
//    }
//    for (i <- metroLineArray.indices) {
//      if (metroLineArray(i).split(",")(0) == home) departureArray += metroLineArray(i).split(",")(2)
//    }
//    val tripArray = new ArrayBuffer[String]
//    for (i <- homeArray.indices) {
//      for (j <- trip.indices) {
//        if (trip(j) == homeArray(i)) tripArray += trip(j)
//      }
//    }
//    val distinctTrip = tripArray.distinct
//    val timeArray = new Array[Int](distinctTrip.length)
//    for (i <- distinctTrip.indices) {
//      for (j <- tripArray.indices) {
//        if (tripArray(j) == distinctTrip(i)) timeArray(i) += 1
//      }
//    }
//    if (timeArray.length == 0) departureArray.head
//    else distinctTrip(maxForArray(timeArray))
//  }
//
  def maxForArray(tmpArray: Array[Int]):Int = {
      var max = tmpArray.head
      var k = 0
      for (i <- tmpArray.indices) {
        if (tmpArray(i) > max) {
          max = tmpArray(i)
          k= i
        }
      }
      k
  }

//  def maxForTwoArray(twoArray:Array[Array[Int]]):String = {
//    val tmpArray = new ArrayBuffer[Int]
//    for (i <- twoArray.indices) tmpArray += twoArray(i)(1)
//    var max = tmpArray.head
//    var k = 0
//    for (i <- tmpArray.indices) {
//      if (tmpArray(i) > max) {
//        max = tmpArray(i)
//        k = i
//      }
//    }
//    twoArray(k)(0).toString
//  }


  /**
   *
   * @param trip 历史轨迹
   * @param metroLineArray 线路矩阵
   * @param departure 当前出发
   * @param arrive 当前到达
   * @return
   */
  def algorithmThree(trip: ArrayBuffer[String],metroLineArray: Array[String],departure:String,arrive:String,home: String): String = {
    a3 += 1
    var line = ""
    var line1 = ""
    for (i <- metroLineArray.indices) {
      if (arrive == metroLineArray(i).split(",")(0) && home == metroLineArray(i).split(",")(1)) line = metroLineArray(i).split(",")(2)
    }
    for (i <- metroLineArray.indices) {
      if (arrive == metroLineArray(i).split(",")(0) && departure == metroLineArray(i).split(",")(1)) line1 = metroLineArray(i).split(",")(2)
    }
    line
  }


//  /**
//   * 计算所有线路的吸引力
//   * @param rdd
//   * @return
//   */
//  def calculateAttOfLine(rdd: RDD[String]): Array[(String, Int)] = {
//    val att = rdd.map(_.split(",")).map(r => passengerTripListAddFeatures(r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8))).map(x => {
//      val tripArray = x.tripList.split("->")
//      val tmpArray = new ArrayBuffer[String]
//      for (i <- x.tripList.split("->").indices) {
//        tmpArray += tripArray(i)
//      }
//      tmpArray.map((_, 1))
//    }).flatMap(x => x).reduceByKey(_ + _).collect()
//    att
//  }


  /**
   * 算法四 在历史轨迹中寻找下一次出行的路线
   * @param trip  历史轨迹
   * @param weekNum 出发星期
   * @param timeNum 出发时间
   * @return
   */
  def algorithmFour(trip: ArrayBuffer[String], weekNum: ArrayBuffer[Int], timeNum: ArrayBuffer[Int]): String = {
    val attShortMatrix = calculateTShortAndAtt(trip)
    val timeMatrix = calculateTimesBaseCurrent(trip)
    //val weekTimeMatrix = basedCurrentFromWeekAndTime(trip, weekNum, timeNum)
    val weekTimeMatrix = theNextCurrentFromWeekAndTime(trip,weekNum,timeNum)
    val resultArray = Array.ofDim[Double](trip.distinct.size, 2)
    val valueArray = new Array[Double](trip.distinct.size)
    for (i <- trip.distinct.indices) {
      valueArray(i) = (0.5 * attShortMatrix(i)(2) + 0.5 * (timeMatrix(i)(1) + weekTimeMatrix(i)(1))) / attShortMatrix(i)(1).toFloat
    }
    for (i <- trip.distinct.indices) {
      resultArray(i)(0) = trip.distinct(i).toDouble
      resultArray(i)(1) = valueArray(i)
    }
    var max = resultArray(0)(0)
    for (i <- trip.distinct.indices) {
      if (resultArray(i)(1) > max) max = resultArray(i)(1)
    }
    var k = 0
    for (i <- trip.distinct.indices) {
      if (resultArray(i)(1) == max) k = i
    }
    resultArray(k)(0).toInt.toString
  }

//  def algorithmFive(departureArray: ArrayBuffer[String],arriveArray: ArrayBuffer[String],trip: ArrayBuffer[String], weekNum: ArrayBuffer[Int], timeNum: ArrayBuffer[Int], attArray: Array[(String,Int)],metroLineArray: Array[String],transferLineArray: Array[String],weekAndTimeLine: Array[(String, Int)]):String={
//    if (tripNew(trip)) algorithmFiveFirst()
//    else algorithmFiveSecond(trip)
//  }

  def algorithmFive(cardID:String, trip: ArrayBuffer[String],transferLineArray:Array[String],metroLineArray: Array[String],departure:String,arrive:String,attArray:Array[(String,Int)],home:String):String = {
    if (tripNew(trip)) {
      a5One += 1
      algorithmFiveFirst1(metroLineArray,departure,arrive) + "V"
      //algorithmFiveFirst(cardID,trip,arrive,metroLineArray,transferLineArray,attArray) + "V"
    }
    else {
      a5Two += 1
      algorithmFiveSecond(trip,departure,arrive,home,metroLineArray) + "S"
    }
  }

//  def algorithmFiveFirst(cardId:String,trip: ArrayBuffer[String],weekNum:ArrayBuffer[Int],timeNum:ArrayBuffer[Int],clusterArray: Array[((String,String),Int)],clusterResult:Array[String],transferLineArray:Array[String],weekAndTimeLine:Array[(String,Int)],metroLineArray: Array[String],arrive:String):String= {
//    var clusterID = ""
//    var transferLine = ""
//    val theCurrentStatus = (weekNum.last, timeNum.last)
//    val theNextStatus = delegateFunctions.theNextWeekAndTime(theCurrentStatus)
//    val theSameClusterIdArray = new ArrayBuffer[(String,Int)]
//    val weekAndTimeArray = new ArrayBuffer[(String, Int)]
//    for (i <- clusterResult.indices) {
//      if (cardId == clusterResult(i).split(",")(0)) clusterID = clusterResult(i).split(",")(1)
//    }
//    for (i <- transferLineArray.indices) {
//      if (trip.last == transferLineArray(i).split(",")(0)) transferLine = transferLineArray(i).split(",")(1)
//    }
//    for (i <- weekAndTimeLine.indices) {
//      if (theNextStatus == weekAndTimeLine(i)._1.split("=>")(0)) weekAndTimeArray += ((weekAndTimeLine(i)._1.split("=>")(1),weekAndTimeLine(i)._2))
//    }
//    for (i <- clusterArray.indices) {
//      if (clusterID == clusterArray(i)._1._1) theSameClusterIdArray += ((clusterArray(i)._1._2,clusterArray(i)._2))
//    }
//    val departureArray = delegateFunctions.metroLineBaseDeparture(metroLineArray,arrive)
//    val transArray = delegateFunctions.theNextTransferLineBaseCurrent(transferLine, transferLineArray)
//    val transAndWeekTime = delegateFunctions.theIntersectionOfTwoArray(transArray,weekAndTimeArray)
//    val transAndCluster = delegateFunctions.theIntersectionOfTwoArray(transArray, theSameClusterIdArray)
//    val departureAndWeekTime = delegateFunctions.theIntersectionOfTwoArray(departureArray, weekAndTimeArray)
//    val departureAndCluster = delegateFunctions.theIntersectionOfTwoArray(departureArray, theSameClusterIdArray)
//
//    val a = delegateFunctions.theIntersectionOfKeyArray(departureAndWeekTime, departureAndCluster)
//    val b = delegateFunctions.theIntersectionOfKeyArray(transAndWeekTime, transAndCluster)
//
//    delegateFunctions.theMaxForTwoArray(a, b)
//  }

//  def algorithmFiveFirst(cardID: String,trip: ArrayBuffer[String], arrive: String, metroLineArray: Array[String],transferLineArray:Array[String],attArray:Array[(String,Int)]):String = {
//    val departureArray: ArrayBuffer[String] = delegateFunctions.metroLineBaseDeparture(metroLineArray, arrive)
//    val distinctTripArray = delegateFunctions.distinctTripBaseDepartureArray(trip,departureArray)
//    val thirdLineArray = delegateFunctions.theThirdLineFromTransferLineArray(transferLineArray)
//    val theNextLineArray = delegateFunctions.theSameElementOfTwoArray(distinctTripArray, thirdLineArray)
//    val result = delegateFunctions.theIntersectionOfArray(theNextLineArray, attArray)
//    val a = cardID
//    result.sortBy(_._2).last._1
//  }

  def algorithmFiveSecond(trip: ArrayBuffer[String],departure: String,arrive:String,home: String, metroLineArray: Array[String]):String ={
    val matrixArray = new ArrayBuffer[String]()
    var line = ""
    var line1= ""
    for (i <- metroLineArray.indices) {
      if (arrive == metroLineArray(i).split(",")(0) && home == metroLineArray(i).split(",")(1)) line = metroLineArray(i).split(",")(2)
    }
    for (i <- metroLineArray.indices) {
      if (arrive == metroLineArray(i).split(",")(0) && departure == metroLineArray(i).split(",")(1)) line1 = metroLineArray(i).split(",")(2)
    }
    val threshold = 2
    for (i <- trip.indices) {
      matrixArray += trip(i)
    }
    val matrix = constructMarkovMatrix(matrixArray)
    val maxTimeArray = chooseFromMatrix(matrix,trip)
    if (maxTimeArray.head.toInt - maxTimeArray(1).toInt > threshold) {
      a1One += 1
      maxTimeArray(2)
    }
    else {
      a1Two += 1
      line + "Y"
    }
  }

  def algorithmFiveFirst1(metroLineArray: Array[String],departure: String, arrive:String):String = {
    var line = ""
    for (i <- metroLineArray.indices) {
      if (arrive == metroLineArray(i).split(",")(0) && departure == metroLineArray(i).split(",")(1)) line = metroLineArray(i).split(",")(2)
    }
    line
  }



//  /**
//   * 构建贝叶斯预测模型
//   * @param rdd 训练数据集
//   * @return
//   */
//  def bayesModel(rdd: RDD[String]): NaiveBayesModel = {
//    val data = rdd.map(_.split(",")).map(x => x(7) + "," + x(8))
//    val parsedData = data.map(line => {
//      val parts = line.split(",")
//      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(_.toDouble)))
//    })
//    NaiveBayes.train(parsedData, lambda = 1.0, modelType = "multinomial")
//  }

  /**
   * 将数据转化为LabeledPoint
   * @param line 特征字符串
   * @return
   */
  def bayesDataFormat(line: String): LabeledPoint = {
    val parts = line.split(",")
    LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(_.toDouble)))
  }


  /**
   * 计算每条线路的吸引力值和最短距离
   * @param trip 历史轨迹
   * @return
   */
  def calculateTShortAndAtt(trip: ArrayBuffer[String]): Array[Array[Int]] = {
    val tripList = new ListBuffer[String]
    for (i <- trip.indices) {
      tripList += trip(i)
    }
    val matrix = Array.ofDim[Int](tripList.distinct.size, 3)
    val attArray = new Array[Int](tripList.distinct.size)
    for (i <- tripList.distinct.indices) {
      for (j <- trip.indices) {
        if (tripList.distinct(i) == trip(j)) attArray(i) += 1
      }
    }
    val TShortArray = new Array[String](tripList.distinct.size)
    for (i <- tripList.distinct.indices) {
      for (j <- (trip.size - 1) to 0 by (-1)) {
        if (tripList.distinct(i) == trip(j)) TShortArray(i) += (trip.size - j).toString
      }
    }
    val tmp: Array[String] = TShortArray.map(_.substring(4, 5))
    for (i <- tripList.distinct.indices) matrix(i)(0) = tripList.distinct(i).toInt
    for (i <- tripList.distinct.indices) matrix(i)(1) = attArray(i)
    for (i <- tripList.distinct.indices) matrix(i)(2) = tmp(i).toInt

    matrix
  }

  //  /**
  //   * 计算状态转移矩阵中每列次数的和，也就是转移到每条路线的次数
  //   * @param trip 历史轨迹 最后一条为当前轨迹
  //   * @return
  //   */
  //  def calculateTimesFromTransferMatrix(trip: ArrayBuffer[String]):Array[Array[Int]] = {
  //    val matrixArray = new ArrayBuffer[String]
  //    val tripList = new ListBuffer[String]
  //    for (i <- trip.indices) {
  //      matrixArray(i) = trip(i)
  //      tripList += trip(i)
  //    }
  //    val matrix: Array[Array[Int]] = constructMarkovMatrix(matrixArray)
  //    val tmp = new Array[Int](matrix.length)
  //    for (i <- matrix.indices) {
  //      for (j <- matrix.indices) {
  //         tmp(i) += matrix(j)(i)
  //      }
  //    }
  //    val timesMatrix = Array.ofDim[Int](tripList.distinct.size,2)
  //    for (i <- tripList.distinct.indices) timesMatrix(i)(0) = tripList.distinct.distinct(i).toInt
  //    for (i <- tripList.distinct.indices) timesMatrix(i)(1) = tmp(i)
  //    timesMatrix
  //  }


  /**
   * 根据当前轨迹计算转移到各轨迹的次数
   * @param trip 历史轨迹
   * @return
   */
  def calculateTimesBaseCurrent(trip: ArrayBuffer[String]): Array[Array[Int]] = {
    val matrixArray = new ArrayBuffer[String]
    val tripList = new ListBuffer[String]
    for (i <- trip.indices) {
      matrixArray += trip(i)
      tripList += trip(i)
    }
    val currentTrip = trip.last
    val matrix = constructMarkovMatrix(matrixArray)
    val distinctList = tripList.distinct
    var k = 0
    for (i <- distinctList.indices) {
      if (distinctList(i) == currentTrip) k = i
    }
    val timesMatrix = Array.ofDim[Int](distinctList.size, 2)
    for (i <- distinctList.indices) timesMatrix(i)(0) = distinctList(i).toInt
    for (i <- distinctList.indices) timesMatrix(i)(1) = matrix(k)(i)
    timesMatrix
  }


  /**
   * 构建关于时间转移的矩阵
   * @param trip 历史轨迹
   * @param weekNum 历史出发星期
   * @param timeNum 历史出发时间段
   * @return
   */
  def constructWeekAndTimeMatrix(trip: ArrayBuffer[String], weekNum: ArrayBuffer[Int], timeNum: ArrayBuffer[Int]): Array[Array[Int]] = {
    val stateTuple = new ListBuffer[(String, Int, Int)]
    val tripList = new ListBuffer[String]
    for (i <- trip.indices) {
      stateTuple += ((trip(i), weekNum(i), timeNum(i)))
      tripList += trip(i)
    }

    val distinctList = tripList.distinct
    val matrix = Array.ofDim[Int](21, distinctList.size)
    val timeArray = reduceByKeyForArray(stateTuple).sortBy(x => (x._1._2.toString + x._1._3.toString).toInt)
    val weekAndTime = Array((0, 0), (0, 1), (0, 2), (1, 0), (1, 1), (1, 2), (2, 0), (2, 1), (2, 2), (3, 0), (3, 1), (3, 2), (4, 0), (4, 1), (4, 2), (5, 0), (5, 1), (5, 2), (6, 0), (6, 1), (6, 2))
    var m = 0
    var n = 0
    for (i <- timeArray.indices) {
      for (j <- distinctList.indices) {
        if (timeArray(i)._1._1 == distinctList(j)) n = j
      }
      for (k <- weekAndTime.indices) {
        if ((timeArray(i)._1._2, timeArray(i)._1._3) == weekAndTime(k)) m = k
      }
      matrix(m)(n) = timeArray(i)._2
    }
    matrix
  }


  /**
   * 计算矩阵中相同元素出现的次数
   * @param test 输入矩阵
   * @return
   */
  def reduceByKeyForArray(test: ListBuffer[(String, Int, Int)]): Array[((String, Int, Int), Int)] = {
    val testList = test.distinct
    val timeArray = new Array[Int](testList.length)
    for (i <- testList.indices) {
      for (j <- test.indices) {
        if (testList(i) == test(j)) timeArray(i) += 1
      }
    }
    val result = new Array[((String, Int, Int), Int)](testList.length)
    for (i <- testList.indices) result(i) = (test(i), timeArray(i))
    result
  }

//  /**
//   * 从时间转移矩阵中根据当前的时间状态计算出转移到每个轨迹的次数
//   * @param trip 历史轨迹
//   * @param weekNum 历史出发星期
//   * @param timeNum 历史出发时间
//   * @return
//   */
//  def basedCurrentFromWeekAndTime(trip: ArrayBuffer[String], weekNum: ArrayBuffer[Int], timeNum: ArrayBuffer[Int]): Array[Array[Int]] = {
//    val currentState = (weekNum.last, timeNum.last)
//    val weekAndTimeMatrix = constructWeekAndTimeMatrix(trip, weekNum, timeNum)
//    val weekAndTime = Array((0, 0), (0, 1), (0, 2), (1, 0), (1, 1), (1, 2), (2, 0), (2, 1), (2, 2), (3, 0), (3, 1), (3, 2), (4, 0), (4, 1), (4, 2), (5, 0), (5, 1), (5, 2), (6, 0), (6, 1), (6, 2))
//    var k = 0
//    for (i <- weekAndTime.indices) {
//      if (currentState == weekAndTime(i)) k = i
//    }
//    val tripList = trip.distinct
//    val tmpArray = new Array[Int](tripList.size)
//    for (i <- tripList.indices) tmpArray(i) = weekAndTimeMatrix(k)(i)
//    val resultArray = Array.ofDim[Int](tripList.size, 2)
//    for (i <- tripList.indices) {
//      resultArray(i)(0) = tripList(i).toInt
//      resultArray(i)(1) = tmpArray(i)
//    }
//    resultArray
//  }

  /**
   * 根据当前的出发时间，找出下一时间各轨迹转移的次数
   * @param trip 历史记录
   * @param weekNum 出发星期
   * @param timeNum 出发时间
   * @return
   */
  def theNextCurrentFromWeekAndTime(trip: ArrayBuffer[String],weekNum:ArrayBuffer[Int],timeNum:ArrayBuffer[Int]):Array[Array[Int]] = {
    val currentState = (weekNum.last, timeNum.last)
    val weekAndTimeMatrix = constructWeekAndTimeMatrix(trip,weekNum,timeNum)
    val weekAndTime = Array((0, 0), (0, 1), (0, 2), (1, 0), (1, 1), (1, 2), (2, 0), (2, 1), (2, 2), (3, 0), (3, 1), (3, 2), (4, 0), (4, 1), (4, 2), (5, 0), (5, 1), (5, 2), (6, 0), (6, 1), (6, 2))
    var k = 0
    var theNextCurrent = ""
    if (currentState._2 == 0) theNextCurrent = weekNum.last + "," + 1
    else if (currentState._2 == 1) theNextCurrent = weekNum.last + "," + 2
    else if(currentState._2 == 2 && currentState._1 < 6) theNextCurrent = weekNum.last+1 + "," + 0
    else theNextCurrent = 0 + "," + 0
    val nextCurrent = (theNextCurrent.split(",")(0).toInt,theNextCurrent.split(",")(1).toInt)
    for (i <- weekAndTime.indices) {
      if (nextCurrent == weekAndTime(i)) k = i
    }
    val tripList = trip.distinct
    val tmpArray = new Array[Int](tripList.size)
    for (i <- tripList.indices) tmpArray(i) = weekAndTimeMatrix(k)(i)
    val resultArray = Array.ofDim[Int](tripList.size,2)
    for (i <- tripList.indices) {
      resultArray(i)(0) = tripList(i).toInt
      resultArray(i)(1) = tmpArray(i)
    }
      resultArray
  }
}


