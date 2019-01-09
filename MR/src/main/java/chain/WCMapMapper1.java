package chain;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author LaZY（李志一）
 * @data 2019/1/9 - 17:00
 */
public class WCMapMapper1 extends Mapper<LongWritable,Text,Text,IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] arr = value.toString().split(" ");

        Text keyout = new Text();
        IntWritable valueOut = new IntWritable();

        for (String s : arr) {
            keyout.set(s);
            valueOut.set(1);
            context.write(keyout,valueOut);
        }
    }
}