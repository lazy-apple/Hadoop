package comprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author LaZY(李志一)
 * @create 2019-02-13 20:13
 */
public class TestCallLog {
    /***
     * 打印某人通话记录的详细信息
     * @throws IOException
     */
    public void printCallLogs() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tname = TableName.valueOf("ns1:callogs");
        Table table = conn.getTable(tname);
        Scan scan = new Scan();

        String callerId = "1381111111";
        String month = "201703";

        String regNo = Util.getRegNo(callerId, month);
        String startKey = regNo + "," + callerId + "," + month;
        scan.setStartRow(Bytes.toBytes(startKey));
        String stopKey = regNo + "," + callerId + "," + month+1;
        scan.setStopRow(Bytes.toBytes(stopKey));

        ResultScanner rs = table.getScanner(scan);
        Iterator<Result> it = rs.iterator();
        while (it.hasNext()){
            String row = Bytes.toString(it.next().getRow());
            System.out.println(row);

            //使用协处理器，每次get数据后，判断主叫被叫
            //主叫：获取列的信息
            //被叫：主叫被叫互换位置，算hash得区域号，然后同上。
        }

    }
}
