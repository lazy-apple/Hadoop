package stormdemo.wc;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import stormdemo.Util;

import java.util.HashMap;
import java.util.Map;
/**
 * @author LaZY(李志一)
 * @create 2019-02-17 13:45
 */
public class CountBolt implements IRichBolt{
    private Map<String,Integer> map ;

    private TopologyContext context;
    private OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        Util.sendToClient(this, "prepare()",9999);
        this.context = context;
        this.collector = collector;
        map = new HashMap<String, Integer>();
    }

    public void execute(Tuple tuple) {
        Util.sendToClient(this, "execute()",9999);
        String word = tuple.getString(0);
        Integer count = tuple.getInteger(1);
        if(!map.containsKey(word)){
            map.put(word,1);
        }
        else{
            map.put(word,map.get(word) + count);
        }
    }

    public void cleanup() {
        for(Map.Entry<String,Integer> entry : map.entrySet()){
            System.out.println(entry.getKey() + " : " + entry.getValue());
            System.out.println("==========================================================================================================");
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
