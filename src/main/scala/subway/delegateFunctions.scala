package subway

import scala.collection.mutable.{ListBuffer, ArrayBuffer}

/**一些公用的方法
 * Created by Flyln on 16/5/12.
 */
object delegateFunctions {

  /**
   * 根据当前的出发星期和时间，来确定下一次的出发时间状态
   * @param theCurrentStatus 当前状态
   * @return
   */
  def theNextWeekAndTime(theCurrentStatus:(Int,Int)):String = {
    if (theCurrentStatus._2 == 0) theCurrentStatus._1 + "->" + 1
    else if (theCurrentStatus._2 == 1) theCurrentStatus._1 + "->" + 2
    else if(theCurrentStatus._2 == 2 && theCurrentStatus._1 < 6)  theCurrentStatus._1+1 + "->" + 0
    else 0 + "->" + 0
  }

  /**
   * 找出两个数组相交的部分
   * @param firstArray 数组1
   * @param SecondArray 数组2
   * @return
   */
  def theIntersectionOfTwoArray(firstArray: ArrayBuffer[String], SecondArray: ArrayBuffer[(String,Int)]):ArrayBuffer[(String,Int)] = {
    val sameArray = new ArrayBuffer[(String,Int)]
    for (i <- firstArray.indices) {
      for (j <- SecondArray.indices) {
        if (firstArray(i) == SecondArray(j)._1) sameArray += SecondArray(j)
      }
    }
    sameArray
  }

  /**
   * 找出两个数组相交的部分
   * @param FirstArray 数组1
   * @param SecondArray 数组2
   * @return
   */
  def theIntersectionOfArray(FirstArray: ArrayBuffer[String], SecondArray: Array[(String, Int)]):ArrayBuffer[(String,Int)] = {
    val tmpArray = new ArrayBuffer[(String, Int)]
    for (i <- FirstArray.indices) {
      for (j <- SecondArray.indices) {
        if (FirstArray(i) == SecondArray(j)._1) tmpArray += SecondArray(j)
      }
    }
    tmpArray
  }

  /**
   * 找出两个数组相交的部分，返回其key值最大的
   * @param FirstArray 数组1
   * @param SecondArray 数组2
   * @return
   */
  def theMaxForTwoArray(FirstArray: ArrayBuffer[(String,Int)], SecondArray: ArrayBuffer[(String,Int)]):String = {
    val sameArray = new ArrayBuffer[(String, Int)]
    for (i <- FirstArray.indices) {
      for (j <- SecondArray.indices) {
        if (FirstArray(i)._1 == SecondArray(j)._1) sameArray += ((FirstArray(i)._1,FirstArray(i)._2 + SecondArray(j)._2))
      }
    }
    if (sameArray.isEmpty) SecondArray.sortBy(_._2).last._1
    else SecondArray.sortBy(_._2).last._1
  }


  /**
   * 找出两个有key值的数组相交的部分
   * @param FirstArray 数组1
   * @param SecondArray 数组2
   * @return
   */
  def theIntersectionOfKeyArray(FirstArray: ArrayBuffer[(String,Int)], SecondArray: ArrayBuffer[(String, Int)]):ArrayBuffer[(String, Int)] = {
    val sameArray = new ArrayBuffer[(String, Int)]
    for (i <- FirstArray.indices) {
      for (j <- SecondArray.indices) {
        if (FirstArray(i)._1 == SecondArray(j)._1) sameArray += ((FirstArray(i)._1,FirstArray(i)._2 + SecondArray(j)._2))
      }
    }
    if (sameArray.isEmpty) FirstArray
    else sameArray
  }


  /**
   * 根据上一次的到达车站确定下一次的出发车站，再找出所有符合的路线
   * @param metroLineArray 线路数组
   * @param arrive 上一次的到达车站
   * @return
   */
  def metroLineBaseDeparture(metroLineArray: Array[String],arrive: String):ArrayBuffer[String] = {
    val departureArray = new ArrayBuffer[String]
    for (i <- metroLineArray.indices) {
      if (arrive == metroLineArray(i).split(",")(0)) departureArray += metroLineArray(i).split(",")(2)
    }
    departureArray
  }

  /**
   * 根据当前到达车站的线路，确定下一次换乘线路
   * @param transferLine 当前的换乘记录
   * @param transferLineArray 换乘路线集合
   * @return
   */
  def theNextTransferLineBaseCurrent(transferLine: String,transferLineArray:Array[String]):ArrayBuffer[String] = {
    val transArray = new ArrayBuffer[String]
    for (i <- transferLineArray.indices) {
      if (transferLine.split("->")(1) == transferLineArray(i).split(",")(1).split("->")(0)) transArray += transferLineArray(i).split(",")(0)
    }
    transArray
  }

  /**
   * 在换乘集合找出换乘到3号线的线路集合
   * @param transferLineArray 换乘集合
   * @return
   */
  def theThirdLineFromTransferLineArray(transferLineArray: Array[String]):ArrayBuffer[String] = {
    val thirdLineArray = new ArrayBuffer[String]
    for (i <- transferLineArray.indices) {
      if (transferLineArray(i).split(",")(1).split("->")(1) == "3") thirdLineArray += transferLineArray(i).split(",")(0)
    }
    thirdLineArray
  }

  /**
   * 找出线路集合中除去当前历史线路的集合
   * @param trip 历史轨迹
   * @param metroLineArray 线路集合
   * @return
   */
  def distinctTripBaseMetroLineArray(trip: ArrayBuffer[String], metroLineArray: Array[String]):ArrayBuffer[String] = {
    val distinctTripArray = trip.distinct
    val tripSet = scala.collection.mutable.Set[String]()
    val metroSet = scala.collection.mutable.Set[String]()
    val tmpArray = new ArrayBuffer[String]
    for (i <- distinctTripArray.indices) tripSet += distinctTripArray(i)
    for (i <- metroLineArray.indices) metroSet += metroLineArray(i)
    val a = metroSet.diff(tripSet).toArray
    for (i <- a.indices) tmpArray += a(i)
    tmpArray
  }

  /**
   * 找出两个数组中相同的元素
   * @param FirstArray 数组1
   * @param SecondArray 数组2
   * @return
   */
  def theSameElementOfTwoArray(FirstArray: ArrayBuffer[String], SecondArray: ArrayBuffer[String]):ArrayBuffer[String] = {
    val tmpArray = new ArrayBuffer[String]
    for (i <- FirstArray.indices) {
      for (j <- SecondArray.indices) {
        if (FirstArray(i) == SecondArray(j)) tmpArray += FirstArray(i)
      }
    }
    tmpArray
  }


  /**
   * 从符合出发站台的线路集合里剔除出历史轨迹
   * @param trip 历史轨迹
   * @param DepartureArray 出发站台集合
   * @return
   */
  def distinctTripBaseDepartureArray(trip: ArrayBuffer[String], DepartureArray: ArrayBuffer[String]):ArrayBuffer[String] = {
    val distinctTripArray = trip.distinct
    val tripSet = scala.collection.mutable.Set[String]()
    val departSet = scala.collection.mutable.Set[String]()
    for (i <- distinctTripArray.indices) tripSet += distinctTripArray(i)
    for (j <- DepartureArray.indices) departSet += DepartureArray(j)
    val a = departSet.diff(tripSet).toArray
    val tmpArray = new ArrayBuffer[String]
    for (i <- a.indices) tmpArray += a(i)
    tmpArray
  }


  /**
   * 从最近的5次路线中来确定乘客最近的出行模式是什么
   * 分别有5种出行模式,为ABBA, ABBC, ABAB, ABAC, ABCA, ABCD
   * @param departureArray 出发站台
   * @param arriveArray 到达站台
   * @return
   */
  def patternChoose(departureArray: ArrayBuffer[String], arriveArray: ArrayBuffer[String]):Array[Int] = {
    val baseArray = new ArrayBuffer[String]
    for (i <- (departureArray.length - 10) to (departureArray.length - 6)) baseArray += departureArray(i) + "=>" + arriveArray(i)
    val timeArray = new Array[Int](6)
    for (i <- 0 to (baseArray.length - 2)) {
      if ((baseArray(i).split("=>")(0) == baseArray(i+1).split("=>")(1)) && (baseArray(i).split("=>")(1) == baseArray(i+1).split("=>")(0))) timeArray(0) += 1
      else if ((baseArray(i) != baseArray(i+1)) && (baseArray(i).split("=>")(1) == baseArray(i+1).split("=>")(0))) timeArray(1) += 1
      else if (baseArray(i) == baseArray(i+1)) timeArray(2) += 1
      else if ((baseArray(i) != baseArray(i+1)) && (baseArray(i).split("=>")(0) == baseArray(i+1).split("=>")(0))) timeArray(3) += 1
      else if ((baseArray(i) != baseArray(i+1)) && (baseArray(i).split("=>")(0) == baseArray(i+1).split("=>")(1))) timeArray(4) += 1
      else timeArray(5) += 1
    }
    timeArray
  }

  /**
   * 返回Int型数组中最大的值
   * @param tmpArray Int型数组
   * @return
   */
  def maxElementForArray(tmpArray: Array[Int]):Int = {
    var max = tmpArray(0)
    for (i <- tmpArray.indices) {
      if (max < tmpArray(i))
        max = tmpArray(i)
    }
    max
  }


  /**
   * 构建二阶马尔科夫矩阵，通过现在和前一时刻的路线来确定下一次的路线
   * @param trip 历史轨迹
   * @return
   */
  def constructSecondOrderMarKovMatrix(trip: ArrayBuffer[String]):Array[Array[Int]] = {
    val tripArray = new ArrayBuffer[String]
    for (i <- 0 to (trip.length - 2)) tripArray += trip(i) + ","  + trip(i+1)
    val distinctArray = tripArray.distinct
    val distinctArray1 = trip.distinct
    val resultArray = Array.ofDim[Int](distinctArray.length, distinctArray1.length)
    var m = 0
    var n = 0
    for (i <- 0 to (trip.length - 3)) {
      for (k <- distinctArray.indices) {
        if (trip(i) + "," + trip(i+1) == distinctArray(k)) m = k
      }
      for (j <- distinctArray1.indices) {
        if (trip(i+2) == distinctArray1(j)) n = j
      }
      resultArray(m)(n) += 1
    }
    resultArray
  }

  /**
   * 从二阶马尔科夫矩阵中选出当前转移次数最多的路线
   * @param matrix 二阶矩阵
   * @param trip 历史轨迹
   * @return
   */
  def chooseFromSecondMatrix(matrix: Array[Array[Int]], trip: ArrayBuffer[String]):String = {
    val currentLine = trip.init.last + "," + trip.last
    val tripArray = new ArrayBuffer[String]
    var k = 0
    for (i <- 0 to (trip.length - 2)) tripArray += trip(i) + ","  + trip(i+1)
    val distinctArray = tripArray.distinct
    for (i <- distinctArray.indices) {
      if (currentLine == distinctArray(i)) k = i
    }
    val timeArray = new Array[Int](trip.distinct.length)
    for (i <- trip.distinct.indices) timeArray(i) = matrix(k)(i)
    val max = delegateFunctions.maxElementForArray(timeArray)
    var tmp = 0
    for (i <- timeArray.indices) {
      if (max == timeArray(i)) tmp = i
    }
    trip.distinct(tmp)
  }


  /**
   * 根据出发星期和时间构建关于时间的状态转移矩阵
   * @param trip 历史轨迹
   * @param weekNum 出发星期
   * @param timeNum 出发时间
   * @return
   */
  def constructWeekAndTimeMatrix(trip: ArrayBuffer[String], weekNum: ArrayBuffer[Int], timeNum: ArrayBuffer[Int]):Array[Array[Int]] = {
    val distinctTrip = trip.distinct
    val weekAndTime = Array((0, 0), (0, 1), (0, 2), (1, 0), (1, 1), (1, 2), (2, 0), (2, 1), (2, 2), (3, 0), (3, 1), (3, 2), (4, 0), (4, 1), (4, 2), (5, 0), (5, 1), (5, 2), (6, 0), (6, 1), (6, 2))
    val resultArray = Array.ofDim[Int](weekAndTime.length, distinctTrip.length)
    var m = 0
    var n = 0
    for (i <- 0 to (trip.length - 2)) {
      for (j <- weekAndTime.indices) {
        if ((weekNum(i),timeNum(i)) == weekAndTime(j)) m = j
      }
      for (k <- distinctTrip.indices) {
        if (trip(i+1) == distinctTrip(k)) n = k
      }
      resultArray(m)(n) += 1
    }
    resultArray
  }

  /**
   * 从时间转移矩阵中选出转移次数最多的线路
   * @param trip 历史轨迹
   * @param matrix 时间转移矩阵
   * @param weekNum 出发星期
   * @param timeNum 出发时间
   * @return
   */
  def chooseFromWeekAndTimeMatrix(trip: ArrayBuffer[String], matrix: Array[Array[Int]], weekNum: ArrayBuffer[Int], timeNum: ArrayBuffer[Int]):ArrayBuffer[String] = {
    val currentStatus = (weekNum.last, timeNum.last)
    val weekAndTime = Array((0, 0), (0, 1), (0, 2), (1, 0), (1, 1), (1, 2), (2, 0), (2, 1), (2, 2), (3, 0), (3, 1), (3, 2), (4, 0), (4, 1), (4, 2), (5, 0), (5, 1), (5, 2), (6, 0), (6, 1), (6, 2))
    var tmp = 0
    for (i <- weekAndTime.indices) {
      if (currentStatus == weekAndTime(i)) tmp = i
    }
    val distinctTrip = trip.distinct
    val timeArray = new Array[Int](distinctTrip.length)
    for(i <- distinctTrip.indices) timeArray(i) = matrix(tmp)(i)
    val resultArray = new ArrayBuffer[String]

    val sortArray = timeArray.sortWith(_ > _)
    val max1 = sortArray(0)


    val trip1 = distinctTrip(confirmLocation(max1,timeArray))

    resultArray += trip1 + "->" + max1
  }

  /**
   * 确定一个数在一个数组中的位置
   * @param a 一个整数
   * @param tmpArray 数组
   * @return
   */
  def confirmLocation(a: Int, tmpArray: Array[Int]):Int = {
    var k = 0
    for (i <- tmpArray.indices) {
      if (a == tmpArray(i)) k = i
    }
    k
  }

  /**
   * 返回次数最大的线路
   * @param lineArray 线路数组,包含次数
   * @return
   */
  def returnMaxForWeekAndTimeMatrix(lineArray: ArrayBuffer[String]):String = {
    lineArray.head.split("->")(0)
  }

  /**
   * 找出三条线路在轨迹中的最近距离
   * @param trip 历史轨迹
   * @param resultArray 三条线路集合，包含次数
   * @return
   */
  def tripLengthForWeekAndTime(trip: ArrayBuffer[String], resultArray: ArrayBuffer[String]):String = {
    if (resultArray(1).split("->")(1).toInt == 0 && resultArray(2).split("->")(1).toInt == 0) resultArray.head.split("->")(0)
    else if (resultArray(1).split("->")(1).toInt != 0 && resultArray(2).split("->")(1).toInt == 0) {
      val tShort1 = tShortForTrip(trip,resultArray.head.split("->")(0))
      val tShort2 = tShortForTrip(trip,resultArray(1).split("->")(1))
      if (tShort1 < tShort2) resultArray.head.split("->")(0)
      else resultArray(1).split("->")(0)
    }
    else {
      val tShort1 = tShortForTrip(trip,resultArray.head.split("->")(0))
      val tShort2 = tShortForTrip(trip,resultArray(1).split("->")(1))
      val tShort3 = tShortForTrip(trip,resultArray(2).split("->")(0))
      val tShort = List(tShort1, tShort2, tShort3)
      val min = tShort.min

      if (min == tShort1) resultArray.head.split("->")(0)
      else if (min == tShort2) resultArray(1).split("->")(0)
      else resultArray(2).split("->")(0)
    }
  }

  /**
   * 线路在历史轨迹中的最近距离
   * @param trip 历史轨迹
   * @param tmp 线路
   * @return
   */
  def tShortForTrip(trip: ArrayBuffer[String], tmp: String):Int = {
    var k = 0
    for (i <- (trip.length - 1) to 0 by (-1)) {
      if (trip(i) == tmp) k = trip.length - i
    }
    k
  }

  /**
   * 计算各个时间段线路的次数
   * @param trip 历史轨迹
   * @param weekNum 出发星期
   * @param timeNum 出发时间
   * @return
   */
  def attAboutWeekAndTime(trip: ArrayBuffer[String], weekNum: ArrayBuffer[Int], timeNum: ArrayBuffer[Int]):Array[Array[Int]] = {
    val weekAndTime = Array((0, 0), (0, 1), (0, 2), (1, 0), (1, 1), (1, 2), (2, 0), (2, 1), (2, 2), (3, 0), (3, 1), (3, 2), (4, 0), (4, 1), (4, 2), (5, 0), (5, 1), (5, 2), (6, 0), (6, 1), (6, 2))
    val distinctTrip = trip.distinct
    val resultArray = Array.ofDim[Int](weekAndTime.length, distinctTrip.length)
    var m = 0
    var n = 0
    for (i <- trip.indices) {
      for (j <- weekAndTime.indices) {
        if ((weekNum(i),timeNum(i)) == weekAndTime(j)) m = j
      }
      for (k <- distinctTrip.indices) {
        if (trip(i) == distinctTrip(k)) n = k
      }
      resultArray(m)(n) += 1
    }
    resultArray
  }

  /**
   * 构建一阶MarKov状态转移矩阵
   * @param tripList 历史轨迹
   * @return
   */
  def constructMarkovMatrix(tripList: ArrayBuffer[String]):Array[Array[Int]] = {
    val tmpList = new ListBuffer[String]()
    for (i <- tripList.indices) {
      tmpList += tripList(i)
    }
    val tmp = tmpList.distinct
    val transferMatrix = Array.ofDim[Int](tmp.size,tmp.size)
    var m = 0
    var n = 0
    for (i <- 0 to (tripList.size - 2)) {
      for (k <- tmp.indices) {
        if (tripList(i) == tmp(k)) m = k
      }
      for (j <- tmp.indices) {
        if (tripList(i+1) == tmp(j)) n = j
      }
      transferMatrix(m)(n) += 1
    }
    transferMatrix
  }

  /**
   * 从一阶状态转移矩阵中选取两条次数最高的线路
   * @param matrix 状态转移矩阵
   * @param trip 历史轨迹
   * @return
   */
  def chooseFromMatrix(matrix:Array[Array[Int]],trip:ArrayBuffer[String]):ArrayBuffer[String]={
    val result = new ArrayBuffer[String]
    val tmpList = new ListBuffer[String]
    var tmpRow = 0
    var tmpClu = 0
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
    val a = timeList.sorted.last
    val b = timeList.sorted.init.last
    for (i <- timeList.indices) {
      if (timeList(i) == a) tmpClu = i
    }
    result += a.toString
    result += b.toString
    result += uniqueTmp(tmpClu)
  }

}
