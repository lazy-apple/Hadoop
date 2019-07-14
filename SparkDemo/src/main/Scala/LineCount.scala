import org.apache.spark.{SparkConf, SparkContext}

/** 每行文本出现几次
  *
  * @author LaZY(李志一) 
  * @create 2019-07-14 20:08 
  */
object LineCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("")
    val sc = new SparkContext(conf)
    var lines = sc.textFile("E:\\IDEA_workspace\\Hadoop\\SparkDemo\\src\\main\\Scala\\1.txt")
    lines.map((_, 1)).reduceByKey(_ + _).foreach(m => println(m._1 + ":" + m._2))
  }

}
