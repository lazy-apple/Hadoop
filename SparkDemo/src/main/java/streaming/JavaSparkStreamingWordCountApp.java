package streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Some;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class JavaSparkStreamingWordCountApp {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();
        conf.setAppName("wc");
        conf.setMaster("local[4]");
        //创建Spark流应用上下文
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Seconds.apply(3));

        jsc.checkpoint("file:///d:/scala/check");
        //创建socket离散流
        JavaReceiverInputDStream sock = jsc.socketTextStream("localhost",9999);
        //压扁
        JavaDStream<String> wordsDS = sock.flatMap(new FlatMapFunction<String,String>() {
            public Iterator call(String str) throws Exception {
                List<String> list = new ArrayList<String>() ;
                String[] arr = str.split(" ");
                for(String s : arr){
                    list.add(s);
                }
                return list.iterator();
            }
        });

        //映射成元组
        JavaPairDStream<String,Integer> pairDS = wordsDS.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String,Integer>(s,1);
            }
        }) ;

        JavaPairDStream<String,Integer> jps = pairDS.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            public Optional<Integer> call(List<Integer> v1, Optional<Integer> v2) throws Exception {
                Integer newCount = v2.isPresent() ? v2.get() : 0  ;

                System.out.println("old value : " + newCount);
                for(Integer i : v1){
                    System.out.println("new value : " + i);
                    newCount = newCount +  i;
                }
                return Optional.of(newCount);
            }
        });



        //聚合
        JavaPairDStream<String,Integer> countDS = jps.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });


        //打印
        countDS.print();

        jsc.start();

        jsc.awaitTermination();

        jsc.stop();
    }
}
