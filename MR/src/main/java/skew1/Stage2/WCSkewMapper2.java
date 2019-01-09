package skew1.Stage2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author LaZY（李志一）
 * @data 2019/1/9 - 15:54
 */
public class WCSkewMapper2 extends Mapper<LongWritable,Text,Text,IntWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] arr = value.toString().split("\t");
        context.write(new Text(arr[0]),new IntWritable(Integer.parseInt(arr[1])));
    }
}
