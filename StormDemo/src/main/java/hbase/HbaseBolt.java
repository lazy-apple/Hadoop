package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.Map;

/**
 * @author LaZY(李志一)
 * @create 2019-02-20 9:05
 */
public class HbaseBolt implements IRichBolt {
    Table table;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        Configuration con = HBaseConfiguration.create();
        try {
            Connection connection = ConnectionFactory.createConnection(con);
            TableName tname = TableName.valueOf("ns1:wordcount");
            table = connection.getTable(tname);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void execute(Tuple tuple) {
        String world = tuple.getString(0);
        Integer count = tuple.getInteger(1);

        byte[] rowkey = Bytes.toBytes(world);
        byte[] f = Bytes.toBytes("f1");
        byte[] c = Bytes.toBytes("count");
        try {
            table.incrementColumnValue(rowkey,f,c,count);
        } catch (IOException e) {
        }
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
