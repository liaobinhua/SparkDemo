package test1

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
 * @author binhualiao
 *         Created Time: 2020/10/14 10:31 
 **/
object SparkTopN {

  /**
   * 背景:
   * ⽤户每天在⽹站上的点击，浏览、下单，⽀付⾏为都会产⽣海量的⽇志，这些⽇志数据将被汇总处理、分
   * 析、挖掘与学习，为公司的各种推荐、搜索系统甚⾄公司战略⽬标制定提供数据⽀持。
   * 需求:
   * 计算每天点击，下单，⽀付次数排名前⼗的品类
   */

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkSQL Test")
      .getOrCreate()

    val dataRDD: RDD[String] = spark.sparkContext.textFile("/Users/t101/Project/Java/sparkRDD/src/main/resources/log.txt")
    /**
     * 1. 获取所有的商品品类ID
     */
    val allCategoryIds: RDD[(Long, Long)] = getAllCategoryId(dataRDD)

    /**
     * 2. 分别获取品类的:点击, 下单, 支付 的次数
     */
    val clickCategoryCount: RDD[(Long, Long)] = getClickCategoryCount(dataRDD, 7)
    val orderCategoryCount: RDD[(Long, Long)] = getCategoryCount(dataRDD, 9)
    val payCategoryCount: RDD[(Long, Long)] = getCategoryCount(dataRDD, 11)

    /**
     * 3. 品类ID结果集 与 第二步结果集 进行 left join
     */
    val resultRDD: RDD[(Long, String)] = leftJoinRDD(allCategoryIds, clickCategoryCount, orderCategoryCount, payCategoryCount)

    /**
     * 4. 实现二次排序
     */
    getTopN(resultRDD)

    spark.close()
  }

  /**
   * 获取所有数据的品类ID
   * @param dataRDD
   * @return
   */
  def getAllCategoryId(dataRDD: RDD[String]): RDD[(Long, Long)] = {
    val ids = new mutable.HashSet[(Long, Long)]
    dataRDD.flatMap(line => {
      val fields: Array[String] = line.split(",")
      val clickCategoryId: String = fields(7)
      val orderCategoryId: String = fields(9)
      val payCategoryId: String = fields(11)
      if (clickCategoryId != null && !clickCategoryId.equals("")) {
        ids += ((clickCategoryId.toLong, clickCategoryId.toLong))
      }
      if (orderCategoryId != null && !orderCategoryId.equals("")) {
        val orderItems: Array[String] = orderCategoryId.split("\\^A")
        for (categoryId <- orderItems) {
          ids += ((categoryId.toLong, categoryId.toLong))
        }
      }
      if (payCategoryId != null && !orderCategoryId.equals("")) {
        val payItems: Array[String] = payCategoryId.split("\\^A")
        for (categoryId <- payItems) {
          ids += ((categoryId.toLong, categoryId.toLong))
        }
      }
      ids
    })
  }

  /**
   * 获取点击品类的次数
   * @param dataRDD
   * @param num
   * @return
   */
  def getClickCategoryCount(dataRDD: RDD[String], num: Int): RDD[(Long, Long)] = {
    dataRDD.filter(line => {
      val fields: Array[String] = line.split(",")
      fields(num) != null && !fields(num).equals("")
    }).map(line => {
      val categoryId: Long = line.split(",")(7).toLong
      (categoryId, 1L)
    }).reduceByKey(_+_)
  }

  /**
   * 获取下单或支付品类的次数
   * @param dataRDD
   * @param num
   * @return
   */
  def getCategoryCount(dataRDD: RDD[String], num: Int): RDD[(Long, Long)] = {
    dataRDD.filter(line => {
      val fields: Array[String] = line.split(",")
      fields(num) != null && !fields(num).equals("")
    }).flatMap(line => {
      line.split(",")(num).split("\\^A")
    }).map(categoryId => {
      (categoryId.toLong, 1L)
    }).reduceByKey(_+_)
  }

  def leftJoinRDD(allCategoryIds: RDD[(Long, Long)],
                  clickCategoryIds: RDD[(Long, Long)],
                  orderCategoryIds: RDD[(Long, Long)],
                  payCategoryIds: RDD[(Long, Long)]
                 ): RDD[(Long, String)] = {
    /**
     * Long: 品类ID
     * Long: 品类ID
     * Option: 品类被点击的次数
     */
    val resultRDD: RDD[(Long, (Long, Option[Long]))] = allCategoryIds.leftOuterJoin(clickCategoryIds)
    val resultRDD2: RDD[(Long, (String, Option[Long]))] = resultRDD.map(tuple => {
      val categoryId: Long = tuple._1
      val count: Long = tuple._2._2.getOrElse(0)
      val value = "category_id=" + categoryId + "|click_category_count=" + count
      (categoryId, value)
    }).leftOuterJoin(orderCategoryIds)

    val resultRDD3: RDD[(Long, (String, Option[Long]))] = resultRDD2.map(tuple => {
      val categoryId: Long = tuple._1
      var value: String = tuple._2._1
      val count: Long = tuple._2._2.getOrElse(0)
      value += "|order_category_count=" + count
      (categoryId, value)
    }).leftOuterJoin(payCategoryIds)

    resultRDD3.map( tuple => {
      val categoryId: Long = tuple._1
      var value: String = tuple._2._1
      val count: Long = tuple._2._2.getOrElse(0)
      value += "|pay_category_count=" + count
      (categoryId, value)
    })
  }

  def getTopN(resultRDD: RDD[(Long, String)]) = {
    resultRDD.map(tuple => {
      val categoryIds: Long = tuple._1
      val value: String = tuple._2
      val valueArr: Array[String] = value.split("\\|")
      val clickCount: Long = valueArr(1).split("=")(1).toLong
      val orderCount: Long = valueArr(2).split("=")(1).toLong
      val payCount: Long = valueArr(3).split("=")(1).toLong
      val key = new SortKey(clickCount, orderCount, payCount)
      (key, value)
    }).sortByKey(false)
      .foreach(tuple => {
        println(tuple._2)
      })
  }

}
