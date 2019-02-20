package hbase;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @author LaZY(李志一)
 * @create 2019-02-20 9:17
 */
public class App {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("wcspout",new WordCountSpout(),2).setNumTasks(1);
        builder.setBolt("split-bolt",new SplitBolt(),2).shuffleGrouping("wcspout").setNumTasks(1);
        builder.setBolt("hbase-bolt",new HbaseBolt(),2).shuffleGrouping("split-bolt").setNumTasks(1);

        Config con = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wc",con,builder.createTopology());
    }
}
