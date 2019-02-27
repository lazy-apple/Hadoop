
package mllib;

import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.SparkConf;
// $example off$

public class JavaRecommendationExample {
  public static void main(String[] args) {
    // $example on$
    SparkConf conf = new SparkConf().setAppName("Recommender Example").setMaster("local[4]");
    JavaSparkContext jsc = new JavaSparkContext(conf);

    // Load and parse the data
    String path = "file:///d:/scala/ml/recomm/test.data";
    JavaRDD<String> data = jsc.textFile(path);

    //训练数据集
    JavaRDD<Rating> ratings = data.map(
      new Function<String, Rating>() {
        public Rating call(String s) {
          String[] sarray = s.split(",");
          return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]),
            Double.parseDouble(sarray[2]));
        }
      }
    );

    // Build the recommendation model using ALS
    int rank = 10;
    int numIterations = 10;
    MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, 0.01);

    //提取训练数据的(user,product)信息构成元组
    JavaRDD<Tuple2<Object, Object>> userProducts = ratings.map(
      new Function<Rating, Tuple2<Object, Object>>() {
        public Tuple2<Object, Object> call(Rating r) {
          return new Tuple2<Object, Object>(r.user(), r.product());
        }
      }
    );

    //得到预测值
    JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
      model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD().map(
        new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
          public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r){
            return new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating());
          }
        }
      ));

    //连接训练数据和预测数形成对别元组信息。
    JavaRDD<Tuple2<Double, Double>> ratesAndPreds =
      JavaPairRDD.fromJavaRDD(ratings.map(
        new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
          public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r){
            return new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating());
          }
        }
      )).join(predictions).values();

    //差平方和的均值。
    double MSE = JavaDoubleRDD.fromRDD(ratesAndPreds.map(
      new Function<Tuple2<Double, Double>, Object>() {
        public Object call(Tuple2<Double, Double> pair) {
          Double err = pair._1() - pair._2();
          return err * err;
        }
      }
    ).rdd()).mean();
    System.out.println("Mean Squared Error = " + MSE);

    // Save and load model
    model.save(jsc.sc(), "target/tmp/myCollaborativeFilter");
    MatrixFactorizationModel sameModel = MatrixFactorizationModel.load(jsc.sc(),
      "target/tmp/myCollaborativeFilter");
    // $example off$

    jsc.stop();
  }
}
