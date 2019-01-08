package maxTemp.allsortsecondarysort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区类
 * @author LaZY（李志一）
 * @data 2019/1/8 - 19:50
 */
public class YearPartitioner extends Partitioner<ComboKey,NullWritable>{
    public int getPartition(ComboKey key, NullWritable nullWritable, int i) {
        int year = key.getYear();
        return year % i;//结果为分区位置（0~n-1）
    }
}
