package MySQL;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author LaZY（李志一）
 * @data 2019/1/9 - 19:18
 */
public class WCMapper extends Mapper<LongWritable,MyDBWritable,Text,IntWritable>{
    @Override
    protected void map(LongWritable key, MyDBWritable value, Context context) throws IOException, InterruptedException {
        String line = value.getTxt();
        String[] arr = line.split(" ");
        for (String s : arr) {
            context.write(new Text(s),new IntWritable(1));
        }
    }
}
