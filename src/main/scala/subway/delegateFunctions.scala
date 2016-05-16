package subway

import scala.collection.mutable.ArrayBuffer

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
}
