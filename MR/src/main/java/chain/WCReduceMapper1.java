package chain;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 过滤单词个数
 * @author LaZY（李志一）
 * @data 2019/1/9 - 17:08
 */
public class WCReduceMapper1 extends Mapper<Text,IntWritable,Text,IntWritable> {
    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        if(value.get()>5){
            context.write(key,value);
        }
    }
}
