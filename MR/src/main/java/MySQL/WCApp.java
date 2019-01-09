package MySQL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 实现数据库交互
 * @author LaZY（李志一）
 * @data 2019/1/9 - 19:39
 */
public class WCApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //设置job的各种属性
        job.setJobName("MySQLApp");                        //作业名称
        job.setJarByClass(WCApp.class);                 //搜索类

        //配置数据库信息
        String driverclass = "com.mysql.jdbc.Driver" ;
        String url = "jdbc:mysql://localhost:3306/big4" ;
        String username= "root" ;
        String password = "root" ;
        //设置数据库配置
        DBConfiguration.configureDB(job.getConfiguration(),driverclass,url,username,password);
        //设置数据输入内容
            //记录数用于计算切片个数
        DBInputFormat.setInput(job,MyDBWritable.class,"select id,name,txt from words","select count(*) from words");

        //
        FileOutputFormat.setOutputPath(job,new Path("h:/mr/sql/out"));
//        DBOutputFormat.setOutput(job,"stats","word","c");

        //设置分区类
        job.setMapperClass(WCMapper.class);             //mapper类
        job.setReducerClass(WCReducer.class);           //reducer类

        job.setNumReduceTasks(3);                       //reduce个数

        job.setMapOutputKeyClass(Text.class);           //
        job.setMapOutputValueClass(IntWritable.class);  //

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);     //

        job.waitForCompletion(true);
    }
}
