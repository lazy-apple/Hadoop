import org.apache.spark.{SparkConf, SparkContext}

/**单词统计
  * @author LaZY(李志一) 
  * @create 2019-07-14 13:51 
  */
object WordCount {
  def main(args: Array[String]): Unit = {
//    var conf = new SparkConf()
//      .setMaster("local")
//      .setAppName("worldcount")
//    var sc = new SparkContext(conf)
//    val lines = sc.textFile("E:\\IDEA_workspace\\Hadoop\\SparkDemo\\src\\main\\Scala\\1.txt")
//    var words = lines.flatMap{lines => lines.split(" ")}
//    var pairs = words.map{word => (word,1)}
//    val wordCount = pairs.reduceByKey{_ + _}
//    wordCount.foreach(wordCount => println(wordCount._1 + " "+ wordCount._2))
    test()
  }

  def test(): Unit ={
    val conf = new SparkConf()
      .setAppName("111")
      .setMaster("local")
    var sc = new SparkContext(conf)
    var lines = sc.textFile("E:\\IDEA_workspace\\Hadoop\\SparkDemo\\src\\main\\Scala\\1.txt")
    var words = lines.flatMap(_.split(" "))
    var pairs = words.map((_,1))
    var wordCount = pairs.reduceByKey(_+_)
    wordCount.foreach(wordCount => println(wordCount._1+" : "+wordCount._2))
  }
}
