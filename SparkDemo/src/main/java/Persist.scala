import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author LaZY(李志一) 
  * @create 2019-02-23 13:50 
  */
object Persist {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Persist")
    conf.setMaster("local[4,3]")//可重试3次
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(1 to 20);
    val rdd2 = rdd1.map(e=>{
      println(e)
      e * 2
    })
    rdd2.persist(StorageLevel.DISK_ONLY)
    println(rdd2.reduce(_+_))
    rdd1.unpersist();//删除持久化
    println(rdd2.reduce(_+_))
  }

}
