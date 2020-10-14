package rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author binhualiao
 *         Created Time: 2020/10/10 14:45 
 **/
object RDDTest {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("RDDTest")
    val sparkContext = new SparkContext(sparkConf)
    // collectAsMap
//    val data1 = Array[String]("huangbo", "xuzheng", "xiaoming")
//    val data2 = Array[Int](18, 19, 20)
//    val rdd1 = sparkContext.parallelize(data1)
//    val rdd2 = sparkContext.parallelize(data2)
//    val resultRDD = rdd1.zip(rdd2)
//    val mapResult = resultRDD.collectAsMap()
//    for (y <- mapResult) {
//      println(y._1)
//      println(y._2)
//    }

    val names = List("dog", "cat", "gnu", "salmon", "rabbit", "turkey", "wolf", "bear", "bee")
    val nameRDD: RDD[String] = sparkContext.parallelize(names)
    // count
//    val nameRDDRes: Long = nameRDD.count()
//    println(nameRDDRes)
    // first
    def myfun(index: Int, iter: Iterator[Int]): Iterator[String] = {
      iter.map(x => "[partID:" + index + ", val: " + x + "]")
    }
//    nameRDD.mapPartitionsWithIndex(myfun).foreach(println)
//    val resultValue: String = nameRDD.first()
//    println(resultValue)
    val data = 1 to 10

    /**
     * sample参数解释：
     * 1、withReplacement：元素可以多次抽样(在抽样时替换)
     * 2、fraction：期望样本的大小作为RDD大小的一部分，
     *      当withReplacement=false时：选择每个元素的概率;分数一定是[0,1] ；
     *      当withReplacement=true时：选择每个元素的期望次数; 分数必须大于等于0。
     * 3、seed：随机数生成器的种子
     */
//    val rdd: RDD[Int] = sparkContext.parallelize(data, 3)
//    rdd.mapPartitionsWithIndex(myfun).foreach(println)
//    val sampleArray: Array[Int] = rdd.takeSample(true, 10)
//
//    sampleArray.foreach(println)
//    println("随机抽取的元素的个数：" + sampleArray.count(x => true))
  }

}
