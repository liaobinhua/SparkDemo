package rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author binhualiao
 *         Created Time: 2020/10/7 16:41 
 **/
object RDD_collect {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("RDD_collect")
    val sparkContext = new SparkContext(sparkConf)

    val word = List("A", "B", "C", "D", "E", "A", "C")
    val rdd = sparkContext.parallelize(word, 2)
    val resultRDD: Array[String] = rdd.collect()
    println(resultRDD.mkString(","))
  }

}
