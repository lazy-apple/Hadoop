package maxTemp.allsortsecondarysort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author LaZY（李志一）
 * @data 2019/1/8 - 16:14
 */
public class MaxTempMapper extends Mapper<LongWritable, Text, ComboKey, NullWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] arr = line.split(" ");
        ComboKey keyout = new ComboKey();
        keyout.setYear(Integer.parseInt(arr[0]));
        keyout.setTemp(Integer.parseInt(arr[1]));
        context.write(keyout,NullWritable.get());
    }
}
