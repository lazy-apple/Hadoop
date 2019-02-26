package SQL;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * jdbc
 * @author LaZY(李志一)
 * @create 2019-02-26 9:27
 */
public class SqlJDBC {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("SqlJDBC");
        SparkSession session = SparkSession.builder().
                                appName("SqlJDBC").
                                config("spark.master", "local").
                                getOrCreate();
        String url = "jdbc:mysql://localhost:3306/dbtest";
        String table = "student";
        Dataset<Row> df = session.read().format("jdbc")
                            .option("url",url)
                            .option("dbtable",table)
                            .option("user","root")
                            .option("password","root")
                            .option("driver","com.mysql.jdbc.Driver")
                            .load();
        df.show();
//        //投影查询
//        Dataset<Row> df2 = df.select(new Column("name"),new Column("phone"));
//        //过滤
//        df2 = df2.where("phone like '135%'");
//        //去重
//        df2 = df2.distinct();


        Properties prop = new Properties();
        prop.put("user", "root");
        prop.put("password", "root");
        prop.put("driver", "com.mysql.jdbc.Driver");

        //写入
        df.write().jdbc(url,"subpersons",prop);
        df.show();
    }
}
