package maxTemp.allsort;

import maxTemp.mr.compress.MaxTempApp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.IOException;

/**
 * mr：全排序
 * 查找每年的最高气温
 * @author LaZY（李志一）
 * @data 2019/1/8 - 16:13
 */
public class MaxTemApp {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");

        Job job = Job.getInstance(conf);

        //设置job的各种属性
        job.setJobName("MaxTempApp");                        //作业名称
        job.setJarByClass(MaxTempApp.class);                 //搜索类
        job.setInputFormatClass(SequenceFileInputFormat.class); //设置输入格式

        //添加输入路径
        FileInputFormat.addInputPath(job,new Path(args[0]));
        //设置输出路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));


        job.setMapperClass(MaxTempMapper.class);             //mapper类
        job.setReducerClass(MaxTempReducer.class);           //reducer类


        job.setMapOutputKeyClass(IntWritable.class);        //
        job.setMapOutputValueClass(IntWritable.class);      //

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);         //


        //创建随机采样器对象
        //freq:每个key被选中的概率
        //numSapmple:抽取样本的总数
        //maxSplitSampled:最大采样切片数
        InputSampler.Sampler<IntWritable, IntWritable> sampler =
                new InputSampler.RandomSampler<IntWritable, IntWritable>(1, 6000, 3);

        job.setNumReduceTasks(3);                           //reduce个数

        //将sample数据写入分区文件.
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(),new Path("file:///h:/mr/par.lst"));

        //设置全排序分区类
        job.setPartitionerClass(TotalOrderPartitioner.class);

        InputSampler.writePartitionFile(job, sampler);
        job.waitForCompletion(true);
    }
}
