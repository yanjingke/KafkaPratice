package kafka.orderKafkaAndStorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.google.gson.Gson;
import order.OrderInfo;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OrderKafkaAndStorm {
    public static void main(String[] args) {
        TopologyBuilder topologyBuilder=new TopologyBuilder();
        topologyBuilder.setSpout("orderKafkaSpout",new KafkaSpout(new SpoutConfig(new ZkHosts("hadoop:2181")
                ,"OrderMq","/Orderkafka","orderKafkaSpout")),
                1);
        topologyBuilder.setBolt("orderBolt",new OrderBolt(),1).shuffleGrouping("orderKafkaSpout") ;
        Config config=new Config();
        LocalCluster localCluster=new LocalCluster();
        localCluster.submitTopology("orderCount",config,topologyBuilder.createTopology());
    }
}
class OrderBolt extends BaseRichBolt{
    OutputCollector outputCollector;
    private JedisPool pool;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
     this.outputCollector=outputCollector;
        JedisPoolConfig config = new JedisPoolConfig();
        //控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
        config.setMaxIdle(5);
        //控制一个pool可分配多少个jedis实例，通过pool.getResource()来获取；
        //如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
        //在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
        config.setMaxTotal(1000 * 100);
        //表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
        config.setMaxWaitMillis(30);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        /**
         *如果你遇到 java.net.SocketTimeoutException: Read timed out exception的异常信息
         *请尝试在构造JedisPool的时候设置自己的超时值. JedisPool默认的超时时间是2秒(单位毫秒)
         */
        pool = new JedisPool(config, "hadoop", 6379);

    }

    @Override
    public void execute(Tuple tuple) {
        Jedis jedis=pool.getResource();
        String line=new String((byte[]) tuple.getValue(0));
        OrderInfo orderInfo = new Gson().fromJson(line, OrderInfo.class);
        jedis.incrBy("totalAmount",orderInfo.getProductPrice());
        String type = getBubyProductId(orderInfo.getProductId(), "b");
        jedis.incrBy(type+"Amount",orderInfo.getProductPrice());
        System.out.println(jedis.get("totalAmount"));
        jedis.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
    public String getBubyProductId(String productId,String type){
        Jedis jedis=pool.getResource();
        List<String> list = jedis.hmget("order", type);
        return list.get(0);
    }
}