package comprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author LaZY(李志一)
 * @create 2019-02-12 17:08
 */
public class Util {
    /***
     * 向主叫表插入数据；
     * 设计rowkey，插入数据
     * @throws Exception
     */
    @Test
    public void put() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tname = TableName.valueOf("ns1:calllogs");//表名
        Table table = conn.getTable(tname);

        String callerId = "13845456767" ;//主叫
        String calleeId = "139898987878" ;//被叫
        SimpleDateFormat sdf = new SimpleDateFormat();
        sdf.applyPattern("yyyyMMddHHmmss");
        String callTime = sdf.format(new Date());//拨号时刻
        int duration = 100 ;
        DecimalFormat dff = new DecimalFormat();
        dff.applyPattern("00000");
        String durStr = dff.format(duration);//通话时长

        //区域00-99
        int hash = (callerId + callTime.substring(0, 6)).hashCode();
        hash = (hash & Integer.MAX_VALUE) % 100 ;

        //hash区域号
        DecimalFormat df = new DecimalFormat();
        df.applyPattern("00");
        String regNo = df.format(hash);

        //拼接rowkey
        //xx , callerid , time ,  direction, calleid  ,duration
        String rowkey = regNo + "," + callerId + "," + callTime + "," + "0," + calleeId + "," + durStr  ;
        byte[] rowid = Bytes.toBytes(rowkey);
        Put put = new Put(rowid);
        put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("callerPos"),Bytes.toBytes("河北"));
        put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("calleePos"),Bytes.toBytes("河南"));
        //执行插入
        table.put(put);
        System.out.println("over");
    }
}
