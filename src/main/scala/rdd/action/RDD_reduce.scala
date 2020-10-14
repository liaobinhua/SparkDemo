package rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author binhualiao
 *         Created Time: 2020/10/6 19:39 
 **/
object RDD_reduce {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    sparkConf.set("spark.driver.host", "127.0.0.1")
    sparkConf.setAppName("RDD_reduce")
    val sc = new SparkContext(sparkConf)
    println(sc)
    this.reduce(sc)

  }

  def reduce(sc: SparkContext): Unit = {
    val list = List(4, 5, 6, 7, 8, 9, 10, 11, 12)
    val rdd = sc.parallelize(list)
    val result = rdd.reduce((x, y ) => x + y)
    println(result)

    val fun = (x:Int, y:Int) => {
      println("运行细节" + (x, y))
      if (y % 2 == 0) {
        x - y
      } else {
        x + y
      }
    }

    println(rdd.reduce(fun))

  }

}
