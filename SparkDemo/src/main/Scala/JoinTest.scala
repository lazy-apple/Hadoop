import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author LaZY(李志一) 
  * @create 2019-07-14 21:12 
  */
object JoinTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("")
    val sc = new SparkContext(conf)
    var map1 = Array(("tom",100 ),("jerry",10),("nick",10),("jerry",111111))
    var map2 = Array(("tom",1 ),("jerry",3),("nick",73),("jerry",11222222))

    sc.parallelize(map1).join(sc.parallelize(map2)).foreach(a => println(a._1+" : " +a._2))
  }

}
