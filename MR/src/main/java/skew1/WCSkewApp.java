package skew1;

import maxTemp.allsort.MaxTempMapper;
import maxTemp.allsort.MaxTempReducer;
import maxTemp.mr.compress.MaxTempApp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 解决数据倾斜问题，统计单词个数（本类为随机分配，最终结果需要在Stage2包下执行）
 * @author LaZY（李志一）
 * @data 2019/1/9 - 15:00
 */
public class WCSkewApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");
        Job job = Job.getInstance(conf);
        //设置job的各种属性

        job.setJobName("WCSkewApp");                        //作业名称
        job.setJarByClass(WCSkewApp.class);                 //搜索类
        job.setInputFormatClass(TextInputFormat.class); //设置输入格式

        //添加输入路径
        FileInputFormat.addInputPath(job,new Path("h:/mr/skew"));
        //设置输出路径
        FileOutputFormat.setOutputPath(job,new Path("h:/mr/skew/out"));

        job.setPartitionerClass(RandomPatitioner.class);

        job.setMapperClass(WCSkewMapper.class);             //mapper类
        job.setReducerClass(WCSkewReducer.class);           //reducer类
        job.setNumReduceTasks(4);

        job.setMapOutputKeyClass(Text.class);        //
        job.setMapOutputValueClass(IntWritable.class);      //

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);         //

        job.waitForCompletion(true);
    }
}
