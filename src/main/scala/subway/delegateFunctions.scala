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
    if (sameArray.isEmpty) FirstArray.sortBy(_._2).last._1
    else SecondArray.sortBy(_._2).last._1
  }


}
