package stormSpouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import kafkaTools.ConsumerGroup;
import kafkaTools.ConsumerThread;
import kafkaTools.stormConsumer;
import stormBolts.KafkaProperties;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * 〈功能简述〉
 * 〈〉
 *
 * @author zhuji
 * @create 2018/6/11
 * @since 1.0.0
 */
public class extractkafkaSpout implements IRichSpout {

//    String topic = null;
//    public extractkafkaSpout(String topic)
//    {
//        this.topic=topic;
//    }

    private static final long serialVersionUID = 7361309030430357253L;
    Integer TaskId=null;
    Queue<String> queue = new ConcurrentLinkedDeque<String>();
    private SpoutOutputCollector collector = null;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        TaskId = topologyContext.getThisTaskId();
        ConsumerGroup consumerGroup = new ConsumerGroup(KafkaProperties.brokers,KafkaProperties.groupId,KafkaProperties.topic,1);
        consumerGroup.start();
        queue=ConsumerThread.getQueue();
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }

    public void nextTuple() {
        if(queue.size()>0)
        {
            String strInfo =queue.poll();
            String[] words = strInfo.split(",");
            System.err.println("TaskId:"+TaskId +";    emit_str:"+strInfo);
            collector.emit(new Values(words[0],words[1],words[2]));
        }
    }

    public void ack(Object o) {
    }

    public void fail(Object o) {
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("VehicleNum","PlateColor","RecordTime"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}