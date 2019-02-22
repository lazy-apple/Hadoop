import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * 单词统计（解决数据倾斜问题）
  * @author LaZY(李志一) 
  * @create 2019-02-22 16:18 
  */
object DataLeanDemo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WordCountScala")
    conf.setMaster("Local[4]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile(" ",4)
    rdd1.flatMap(_.split(" ")).map((_,1)).map(t=> { //每一行按空格切，压扁
      val word = t._1//取第一个单词
      val r = Random.nextInt(100)
      (word + "_" + r,1)//加随机数用于分区
    }).reduceByKey(_+_,4).map(t=>{
      val word = t _1;
      val count = t _2;
      val w = word.split("_")(0)
      (w,count)//删掉用于分区的字符
    }).reduceByKey(_+_,4).saveAsTextFile("")//保存结果


  }
}
