package stormTopology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import stormBolts.KafkaProperties;
import stormBolts.loadHdfsBolt;
import stormSpouts.extractkafkaSpout;

/**
 * 〈功能简述〉
 * 〈〉
 *
 * @author zhuji
 * @create 2018/6/11
 * @since 1.0.0
 */
public class dataEtlTopology {

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("extractkafkaSpout", new extractkafkaSpout(), 1);
        builder.setBolt("loadHdfsBolt", new loadHdfsBolt(), 1).shuffleGrouping("extractkafkaSpout");

        Config config = new Config();
        config.setDebug(false);

        if(args.length>0)
        {
            try {
                try {
                    StormSubmitter.submitTopology(args[0],config,builder.createTopology());
                } catch (AuthorizationException e) {
                    e.printStackTrace();
                }
            }
            catch (AlreadyAliveException e)
            {
                e.printStackTrace();
            }
            catch (InvalidTopologyException e)
            {
                e.printStackTrace();
            }
        }
        else
        {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("myTopology",config,builder.createTopology());
        }

    }
}