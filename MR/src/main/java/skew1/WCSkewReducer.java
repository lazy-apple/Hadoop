package skew1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author LaZY（李志一）
 * @data 2019/1/9 - 15:00
 */
public class WCSkewReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable iw : values) {
            count = count + iw.get();
        }
        context.write(key,new IntWritable(count));
    }
}
