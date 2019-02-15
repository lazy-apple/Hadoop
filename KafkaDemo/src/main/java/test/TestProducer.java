package test;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.controller.KafkaController;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
/**
 * @author LaZY(李志一)
 * @create 2019-02-15 14:47
 */
public class TestProducer {
    @Test
    public void testSend(){
        Properties props = new Properties();
        //broker列表
        props.put("metadata.broker.list", "s202:9092");
        //串行化
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //
        props.put("request.required.acks", "1");

        //创建生产者配置对象
        ProducerConfig config = new ProducerConfig(props);

        //创建生产者
        Producer<String, String> producer = new Producer<String, String>(config);

        KeyedMessage<String, String> msg = new KeyedMessage<String, String>("test3","100" ,"hello world tomas100");
        producer.send(msg);
        System.out.println("send over!");
    }
    /**
     * 消费者
     */
    @Test
    public void testConumser(){
        //
        Properties props = new Properties();
        props.put("zookeeper.connect", "s202:2181");
        props.put("group.id", "g3");
        props.put("zookeeper.session.timeout.ms", "500");
        props.put("zookeeper.sync.time.ms", "250");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
        //创建消费者配置对象
        ConsumerConfig config = new ConsumerConfig(props);
        //
        Map<String, Integer> map = new HashMap<String, Integer>();
        map.put("test3", new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> msgs = Consumer.createJavaConsumerConnector(new ConsumerConfig(props)).createMessageStreams(map);
        List<KafkaStream<byte[], byte[]>> msgList = msgs.get("test3");
        for(KafkaStream<byte[],byte[]> stream : msgList){
            ConsumerIterator<byte[],byte[]> it = stream.iterator();
            while(it.hasNext()){
                byte[] message = it.next().message();
                System.out.println(new String(message));
            }
        }
    }
}
