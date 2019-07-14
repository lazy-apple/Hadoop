import org.apache.spark.{SparkConf, SparkContext}

/**按学生姓名分组，打印成绩
  * @author LaZY(李志一) 
  * @create 2019-07-14 20:55 
  */
object GroupByKeyTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("")
    val sc = new SparkContext(conf)
    var map = Array(("tom",100 ),("jerry",10),("tom",10),("jerry",14),("nick",70))
    sc.parallelize(map).groupByKey().foreach(a => {print(a._1);print(" ");a._2.foreach(b =>print(b +" "));println()})
  }

}
