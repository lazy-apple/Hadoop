import org.apache.spark.{SparkConf, SparkContext}

/**map算子，元素都乘以2
  * @author LaZY(李志一) 
  * @create 2019-07-14 20:18 
  */
object mapTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("")
    val sc = new SparkContext(conf)
    var list = Array(1,2,3,44,5)
    sc.parallelize(list).map(_*2).foreach(println(_))

  }

}
