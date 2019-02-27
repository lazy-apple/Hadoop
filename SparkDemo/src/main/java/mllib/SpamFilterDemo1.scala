package mllib

/**垃圾邮件
  * @author LaZY(李志一) 
  * @create 2019-02-27 10:31 
  */
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{HashingTF, RegexTokenizer, StopWordsRemover, Tokenizer, Word2Vec}

object SpamFilterDemo1 {
  def main(args: Array[String]): Unit = {
    val sess = SparkSession.builder().appName("ml").master("local[4]").getOrCreate();
    val sc = sess.sparkContext;

    //垃圾邮件训练数据
    val training = sess.createDataFrame(Seq(
      ("you@example.com", "hope you are well", 0.0),
      ("raj@example.com", "nice to hear from you", 0.0),
      ("thomas@example.com", "happy holidays", 0.0),
      ("mark@example.com", "see you tomorrow", 0.0),
      ("dog@example.com", "save loan money", 1.0),
      ("xyz@example.com", "save money", 1.0),
      ("top10@example.com", "low interest rate", 1.0),
      ("marketing@example.com", "cheap loan", 1.0)))
      .toDF("email", "message", "label")

    //分词器,指定输入列，生成输出列
    val tokenizer = new Tokenizer().setInputCol("message").setOutputCol("words")
    //哈希词频
    val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol("words").setOutputCol("features")
    //创建逻辑回归对象
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.01)
    //设置管线
    val pipeline = new Pipeline().setStages(Array(tokenizer,hashingTF, lr))
    //拟合，产生模型
    val model = pipeline.fit(training)
    //测试数据，评判model的质量
    val test = sess.createDataFrame(Seq(
      ("you@example.com", "ab how are you"),
      ("jain@example.com", "ab hope doing well"),
      ("caren@example.com", "ab want some money"),
      ("zhou@example.com", "ab secure loan"),
      ("ted@example.com", "ab need loan"))).toDF("email", "message")

    //对测试数据进行模型变换,得到模型的预测结果
    val prediction = model.transform(training).select("email", "message", "prediction")
    //prediction.show()

    //类似于切割动作。
    val wordsDF = tokenizer.transform(training)
    //wordsDF.show()

    val featurizedDF = hashingTF.transform(wordsDF)
    featurizedDF.show()
  }
}
