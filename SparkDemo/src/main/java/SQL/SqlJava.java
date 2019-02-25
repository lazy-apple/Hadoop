package SQL;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;

import java.util.function.Consumer;

/**
 * @author LaZY(李志一)
 * @create 2019-02-25 19:45
 */
public class SqlJava {
    public static void main(String[] args) throws AnalysisException {
        SparkConf conf = new SparkConf();
        conf.setAppName("sqljava");
        conf.setMaster("local");
        SparkSession session = SparkSession.builder()
                                .appName("sqljava")
                                .config("spark.master","local")
                                .getOrCreate();//用于创建数据框
        Dataset<Row> df = session.read().json("E:\\IDEA_workspace\\Hadoop\\SparkDemo\\src\\main\\java\\json\\json.dat");
//        df.show();
        df.createOrReplaceTempView("customers");//创建临时视图
        df = session.sql("select * from customers");
        df.show();
//        Dataset<Row> df3 = df.where("age>13");
//        df3.show();
        Dataset<Row> df2 = session.sql("select * from customers where age > 13");
        df2.show();
        //聚合查询
        Dataset<Row> dfCount = session.sql("select count(1) from customers");
        dfCount.show();


//        DataFrame和RDD互操作
        JavaRDD<Row> rdd = df.toJavaRDD();
        rdd.collect().forEach(new Consumer<Row>() {
            public void accept(Row row) {
                long age = row.getLong(0);
                long id = row.getLong(1);
                String name = row.getString(2);
                System.out.println(age + "," + id + "," + name);
            }
        });
        //保存处理,设置保存模式
        df2.write().mode(SaveMode.Append).json("file:///d:/scala/json/out.dat");

    }
}
