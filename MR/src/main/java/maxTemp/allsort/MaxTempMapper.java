package maxTemp.allsort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author LaZY（李志一）
 * @data 2019/1/8 - 16:14
 */
public class MaxTempMapper extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable>{
    protected void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
        context.write(key,value);
    }
}
