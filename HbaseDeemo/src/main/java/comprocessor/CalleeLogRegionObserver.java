package comprocessor;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author LaZY(李志一)
 * @create 2019-02-12 17:03
 */
public class CalleeLogRegionObserver extends BaseRegionObserver {
    /***
     * 协处理器：
     * 用于在插入主叫数据后，插入被叫数据
     */
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        super.postPut(e, put, edit, durability);
        //
        TableName callLogs = TableName.valueOf("calllogs");
        //得到当前的TableName对象
        TableName tableName = e.getEnvironment().getRegion().getRegionInfo().getTable();
        if(!callLogs.equals(tableName)){
            return  ;
        }

        //得到主叫的rowkey
        //xx , callerid , time ,  direction, calleid  ,duration
        //被叫:calleid,time,

        String rowkey = Bytes.toString(put.getRow());
        String[] arr = rowkey.split(",");

//        String hash = Util.getRegNo(arr[4],arr[2]);
//        //hash
//
//        String newRowKey = hash + "," + arr[4] + "," + arr[2] + ",1," + arr[1] + "," +  arr[5] ;
//        Put newPut = new Put(Bytes.toBytes(newRowKey));
//
//        Table t = e.getEnvironment().getTable(tableName);
//
//        t.put(newPut);
    }
}
