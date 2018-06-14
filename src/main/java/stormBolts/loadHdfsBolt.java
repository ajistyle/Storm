package stormBolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import java.util.Map;

/**
 * 〈功能简述〉
 * 〈〉
 *
 * @author zhuji
 * @create 2018/6/11
 * @since 1.0.0
 */
public class loadHdfsBolt implements IRichBolt {

    private static final long serialVersionUID = -4880963572379636724L;
    private OutputCollector collector;
    private FileSystem fileSystem;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    public void execute(Tuple tuple) {
        try
        {
            String line0 = tuple.getString(0).trim();
            String line1 = tuple.getString(1).trim();
            String line2 = tuple.getString(2).trim();
            if(!line0.isEmpty() && !line1.isEmpty() && !line2.isEmpty())
            {
                System.err.println("bolt_line[0]:"+ line0);
                System.err.println("bolt_line[1]:"+ line1);
                System.err.println("bolt_line[2]:"+ line2);
            }
            else
            {
                System.err.println("bolt_line[0]:"+ line0);
            }
//            collector.ack(tuple);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            collector.fail(tuple);
        }
    }

    public void cleanup() {
        try {
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(""));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}