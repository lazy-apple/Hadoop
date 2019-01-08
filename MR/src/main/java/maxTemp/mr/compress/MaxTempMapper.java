package maxTemp.mr.compress;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author LaZY（李志一）
 * @data 2019/1/7 - 19:18
 */
public class MaxTempMapper extends Mapper<LongWritable,Text,IntWritable,IntWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();//解析
        String arr[] = line.split(" ");//切割
        context.write(new IntWritable(Integer.parseInt(arr[0])),new IntWritable(Integer.parseInt(arr[1])));//写入
    }
}
