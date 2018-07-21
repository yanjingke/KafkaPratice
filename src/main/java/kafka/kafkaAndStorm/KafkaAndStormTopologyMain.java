package kafka.kafkaAndStorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import clojure.lang.IFn;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.Map;

public class KafkaAndStormTopologyMain {
    public static void main(String[] args) {
        TopologyBuilder topologyBuilder=new TopologyBuilder();
        topologyBuilder.setSpout("kafkaSpout",
                new KafkaSpout(new SpoutConfig(new ZkHosts("hadoop:2181"),"TestMq","/myKafka","kafkaSpout")),//zkROot这个项目/myKafka"在zookeeper放元数据目录

                1);
        topologyBuilder.setBolt("mybolt1",new MykafkaBolt(),1).shuffleGrouping("kafkaSpout");
        Config config=new Config();
        LocalCluster localCluster=new LocalCluster();
        localCluster.submitTopology("mywordCount",config,topologyBuilder.createTopology());
    }


}
class MykafkaBolt extends BaseRichBolt{
    OutputCollector outputCollector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String line=new String((byte[]) tuple.getValue(0));
        System.out.println(line);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}