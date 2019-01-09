package chain;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author LaZY（李志一）
 * @data 2019/1/9 - 17:00
 */
public class WCMapMapper2 extends Mapper<Text,IntWritable,Text,IntWritable>{
    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        if(!key.toString().equals("falungong")){
            context.write(key,value);
        }
    }
}
