import org.apache.spark.{SparkConf, SparkContext}

/**学生总分
  * @author LaZY(李志一) 
  * @create 2019-07-14 21:07 
  */
object reducebykeyTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("")
    val sc = new SparkContext(conf)
    var map = Array(("tom",100 ),("jerry",10),("tom",10),("jerry",14),("nick",70))
    sc.parallelize(map).reduceByKey(_+_).foreach(a => print(a._1)+" : "+println(a._2))

  }
}
