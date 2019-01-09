package skew1.Stage2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import skew1.WCSkewReducer;

import java.io.IOException;

/**
 * 解决数据倾斜问题，统计单词个数
 * @author LaZY（李志一）
 * @data 2019/1/9 - 15:54
 */
public class WCSkewApp2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");
        Job job = Job.getInstance(conf);
        //设置job的各种属性

        job.setJobName("WCSkewApp2");                        //作业名称
        job.setJarByClass(WCSkewApp2.class);                 //搜索类
        job.setInputFormatClass(TextInputFormat.class); //设置输入格式

        //添加输入路径
        FileInputFormat.addInputPath(job,new Path("h:/mr/skew/out/part-r-00000"));
        FileInputFormat.addInputPath(job,new Path("h:/mr/skew/out/part-r-00001"));
        FileInputFormat.addInputPath(job,new Path("h:/mr/skew/out/part-r-00002"));
        FileInputFormat.addInputPath(job,new Path("h:/mr/skew/out/part-r-00003"));
        //设置输出路径
        FileOutputFormat.setOutputPath(job,new Path("h:/mr/skew/out2"));


        job.setMapperClass(WCSkewMapper2.class);             //mapper类
        job.setReducerClass(WCSkewReducer.class);           //reducer类
        job.setNumReduceTasks(4);

        job.setMapOutputKeyClass(Text.class);        //
        job.setMapOutputValueClass(IntWritable.class);      //

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);         //

        job.waitForCompletion(true);
    }
}
