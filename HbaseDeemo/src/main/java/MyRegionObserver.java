import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * 自定义区域观察者：对于基础操作的开始和结束进行记录
 * @author LaZY(李志一)
 * @create 2019-02-11 13:40
 */
public class MyRegionObserver extends BaseRegionObserver{
    /***
     * 写入信息
     * @param str
     */
    private void outInfo(String str){
        try {
            FileWriter fw = new FileWriter("/home/centos/coprocessor.txt", true);
            fw.write(str+"\r\n");
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start(CoprocessorEnvironment e) throws IOException {
        super.start(e);
        outInfo("MyRegionObserver.start()");
    }

    public void preOpen(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
        super.preOpen(e);
        outInfo("MyRegionObserver.preOpen()");
    }

    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
        super.postOpen(e);
        outInfo("MyRegionObserver.postOpen()");
    }

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
        super.preGetOp(e, get, results);
        String rowkey = Bytes.toString(get.getRow());
        outInfo("MyRegionObserver.preGetOp() : rowkey = " + rowkey);
    }

    public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
        super.postGetOp(e, get, results);
        String rowkey = Bytes.toString(get.getRow());
        outInfo("MyRegionObserver.postGetOp() : rowkey = " + rowkey);
    }

    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        super.prePut(e, put, edit, durability);
        String rowkey = Bytes.toString(put.getRow());
        outInfo("MyRegionObserver.prePut() : rowkey = " + rowkey);
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        super.postPut(e, put, edit, durability);
        String rowkey = Bytes.toString(put.getRow());
        outInfo("MyRegionObserver.postPut() : rowkey = " + rowkey);
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
        super.preDelete(e, delete, edit, durability);
        String rowkey = Bytes.toString(delete.getRow());
        outInfo("MyRegionObserver.preDelete() : rowkey = " + rowkey);
    }

    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
        super.postDelete(e, delete, edit, durability);
        String rowkey = Bytes.toString(delete.getRow());
        outInfo("MyRegionObserver.postDelete() : rowkey = " + rowkey);
    }
}
