import org.apache.spark.{SparkConf, SparkContext}

/**过滤偶数
  * @author LaZY(李志一) 
  * @create 2019-07-14 20:25 
  */
object filter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("")
    val sc = new SparkContext(conf)
    var arr = Array(1,2,3,4,5,6)
    sc.parallelize(arr).filter(_%2 == 0).foreach(println(_))
  }

}
