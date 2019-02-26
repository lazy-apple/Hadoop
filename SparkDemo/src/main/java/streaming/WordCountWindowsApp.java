package streaming;


import com.sun.xml.bind.v2.ContextFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * spark流计算容错+窗口化
 * @author LaZY(李志一)
 * @create 2019-02-26 21:24
 */
public class WordCountWindowsApp {
    static JavaReceiverInputDStream sock;
    public static void main(String[] args) throws InterruptedException {
        Function0<JavaStreamingContext> contextFunction = new Function0<JavaStreamingContext>() {
            @Override
            public JavaStreamingContext call() throws Exception {
                SparkConf conf = new SparkConf();
                conf.setMaster("local[4]");
                conf.setAppName("wc");
                JavaStreamingContext jssc = new JavaStreamingContext(conf,new Duration(2000));
                JavaDStream<String> lines = jssc.socketTextStream("localhost", 9999);
                JavaDStream<Long> dscount = lines.countByWindow(new Duration(24 * 60 * 60 * 1000), new Duration(2000));
                dscount.print();
                jssc.checkpoint("file:///");
                return jssc;
            }
        };
        JavaStreamingContext context = JavaStreamingContext.getOrCreate("file:///", contextFunction);
        context.start();
        context.awaitTermination();
    }
}
