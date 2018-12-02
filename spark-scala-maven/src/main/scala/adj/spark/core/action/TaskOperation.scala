package adj.spark.core.action

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TaskOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
               .setMaster("local")
               .setAppName("TaskOperation")
               
    val sc = new SparkContext(conf)
    val numberArr = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    
    val numberRDD = sc.parallelize(numberArr, 3);
    
    val top3RDD = numberRDD.take(3)
    
    for(top <- top3RDD) {
      println("top 3 = " + top)
    }
  }
}