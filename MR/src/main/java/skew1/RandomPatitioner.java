package skew1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.Random;

/**
 * 自定义分区函数
 * @author LaZY（李志一）
 * @data 2019/1/9 - 15:01
 */
public class RandomPatitioner extends Partitioner<Text,IntWritable>{
    public int getPartition(Text text, IntWritable intWritable, int i) {
        return new Random().nextInt(i);//i:分区个数
    }
}
