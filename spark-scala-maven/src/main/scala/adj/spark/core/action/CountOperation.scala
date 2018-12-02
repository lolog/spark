package adj.spark.core.action

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CountOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
               .setMaster("local")
               .setAppName("CountOperation")
    val sc = new SparkContext(conf)
    val numberArr = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    
    val numberRDD = sc.parallelize(numberArr, 3);
    val douleRDD = numberRDD.map(number => number * 2)
    
    val count = douleRDD.count()
    println("count = " + count)
  }
}