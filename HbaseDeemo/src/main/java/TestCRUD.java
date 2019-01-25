
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import javax.naming.Name;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.Map;

/**
 * @author LaZY(李志一)
 * @create 2019-01-25 12:28
 */
public class TestCRUD {
    /***
     * 向ns1:t1中插入一行（row4）数据
     * @throws Exception
     */
    @Test
    public void put() throws Exception {
        //创建conf对象
        Configuration conf = HBaseConfiguration.create();
        //通过连接工厂创建连接对象
        Connection conn = ConnectionFactory.createConnection(conf);
        //通过连接查询tableName对象
        TableName tname = TableName.valueOf("ns1:t1");
        //获得table
        Table table = conn.getTable(tname);

        //通过bytes工具类创建字节数组(将字符串)
        byte[] rowid = Bytes.toBytes("row4");

        //创建put对象
        Put put = new Put(rowid);

        byte[] f1 = Bytes.toBytes("f1");
        byte[] id = Bytes.toBytes("id") ;
        byte[] value = Bytes.toBytes(102);
        put.addColumn(f1,id,value);

        //执行插入
        table.put(put);
    }

    /***
     * 获取ns1:t1中的数据：
     *  row3中字段为name的值
     * @throws IOException
     */
    @Test
    public void get() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tname = TableName.valueOf("ns1:t1");
        Table table = conn.getTable(tname);

        byte[] rowid = Bytes.toBytes("row3");
        Get get = new Get(rowid);
        Result result = table.get(get);
        byte[] idvalue = result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
        System.out.println(Bytes.toString(idvalue));
    }

    /***
     * 插入十万数据
     * @throws Exception
     */
    @Test
    public void bigInsert()throws Exception{
        long start = System.currentTimeMillis();//当前时间
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:t1");
        HTable table = (HTable) conn.getTable(tableName);

        //提高运行效率一：不要自动清理缓冲区（改为手动控制）
        table.setAutoFlush(false);

        for (int i = 1 ; i < 10000; i++) {
            Put put = new Put(Bytes.toBytes("row" + formatNum().format(i)));
            //提高运行效率二：关闭写前日志
            put.setWriteToWAL(false);
            put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("id"),Bytes.toBytes(i));
            put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("name"),Bytes.toBytes("tom"+i));
            put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("age"),Bytes.toBytes(i%100));
            table.put(put);

            if (i%2000==0){
                table.flushCommits();
            }
        }
        table.flushCommits();
        System.out.println(System.currentTimeMillis()-start);

    }
    /***
     * 格式化文本：用0补位。
     *  原因：hbase中，根据rowkey排序，但是rowkey均为字节数组，每一位的字节进行比较，结果不为理想值，
     *  正确方式是讲所有数值，不满位数的用0补齐。
     * @return
     */
    public static DecimalFormat formatNum(){
        DecimalFormat format = new DecimalFormat();
        format.applyLocalizedPattern("0000");
        return format;
    }
}
