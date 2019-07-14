import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author LaZY(李志一) 
  * @create 2019-07-14 20:51 
  */
object flatMapTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("")
    val sc = new SparkContext(conf)
    var lines = sc.textFile("E:\\IDEA_workspace\\Hadoop\\SparkDemo\\src\\main\\Scala\\1.txt")
    lines.flatMap(_.split(" ")).foreach(println(_))
  }

}
