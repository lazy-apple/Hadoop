package maxTemp.allsort;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;
import java.util.Random;

/**
 * @author LaZY（李志一）
 * @data 2019/1/8 - 16:27
 */
public class Test {
    /***
     * 生成序列文件
     * 产看方式：
     * cmd-->hdfs dfs file:///文件完整路径
     * @throws IOException
     */
    public static void save() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");
        FileSystem fs = FileSystem.get(conf);
        Path p = new Path("h:/mr/maxtemp/1.seq");
        SequenceFile.Writer writer = SequenceFile.createWriter(fs,conf,p,IntWritable.class,IntWritable.class);
        for(int i = 0;i< 6000;i++){
            int year = 1970 + new Random().nextInt(100);
            int temp = -30+new Random().nextInt(100);
            writer.append(new IntWritable(year),new IntWritable(temp));
        }
        writer.close();
    }


    public static void main(String[] args) throws IOException {
        save();
    }
}
