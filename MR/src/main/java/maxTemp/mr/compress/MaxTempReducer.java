package maxTemp.mr.compress;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author LaZY（李志一）
 * @data 2019/1/7 - 19:18
 */
public class MaxTempReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int max = Integer.MIN_VALUE ;
        for(IntWritable iw : values){
            max = max > iw.get() ? max : iw.get() ;
        }
        context.write(key,new IntWritable(max));
    }
}
