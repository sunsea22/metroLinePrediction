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

}
