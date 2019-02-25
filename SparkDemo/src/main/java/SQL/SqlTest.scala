package SQL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author LaZY(李志一) 
  * @create 2019-02-23 15:16 
  */
class SqlTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SqlTest")
    conf.setMaster("local[4,3]")//4条线程，可重试3次
    val sc = new SparkContext(conf)

    case class Customer(id:Int,name:String,age:Int)
    val arr = Array("1,tom,10","2,tomas,21","3,tomas,30")
    val rdd1 = sc.makeRDD(arr)
    val rdd2 = rdd1.map(e=>{
      e.split(",")
      Customer(arr(0).toInt,arr(1).toString,arr(2).toInt)
    })
//    spark.createDataFrame(rdd2)
  }
}
