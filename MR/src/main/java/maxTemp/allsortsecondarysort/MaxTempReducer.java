package maxTemp.allsortsecondarysort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author LaZY（李志一）
 * @data 2019/1/8 - 16:15
 */
public class MaxTempReducer extends Reducer<ComboKey, NullWritable, IntWritable, IntWritable>{
    @Override
    protected void reduce(ComboKey key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        int year = key.getYear();
        int temp = key.getTemp();
        context.write(new IntWritable(year),new IntWritable(temp));
    }
}
