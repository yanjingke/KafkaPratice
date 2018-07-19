package kafka.simple;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.List;
import java.util.Map;

import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaConstomsSimple implements  Runnable{
    public String title;
    public KafkaStream<byte[],byte[]>stream;

    public KafkaConstomsSimple(String title, KafkaStream<byte[], byte[]> stream) {
        this.title = title;
        this.stream = stream;
    }

    public void run() {
        System.out.println("开始运行 " + title);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        /**
         * 不停地从stream读取新到来的消息，在等待新的消息时，hasNext()会阻塞
         * 如果调用 `ConsumerConnector#shutdown`，那么`hasNext`会返回false
         * */
        while (it.hasNext()){
            MessageAndMetadata<byte[], byte[]> data =it.next();
            String topic =data.topic();

            int partition = data.partition();
            long offset = data.offset();
            String msg = new String(data.message());
            System.out.println(String.format(
                    "Consumer: [%s],  Topic: [%s],  PartitionId: [%d], Offset: [%d], msg: [%s]",
                    title, topic, partition, offset, msg));
        }
    }

    public static void main(String[] args) {
        Properties properties=new Properties();
        properties.put("group.id", "dashujujiagoushidd");
        properties.put("zookeeper.connect", "hadoop:2181");
        properties.put("auto.offset.reset", "largest");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("partition.assignment.strategy", "roundrobin");
        ConsumerConfig config=new ConsumerConfig(properties);
        String topic1 = "TestoneMQ";
        //只要ConsumerConnector还在的话，consumer会一直等待新消息，不会自己退出
        ConsumerConnector consumerConnector= Consumer.createJavaConsumerConnector(config);
        Map<String,Integer>topicCoutMap =new HashMap<String,Integer>();
        topicCoutMap.put(topic1,3);
        //Map<String, List<KafkaStream<byte[], byte[]>> 中String是topic， List<KafkaStream<byte[], byte[]>是对应的流
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumerConnector.createMessageStreams(topicCoutMap);
        List<KafkaStream<byte[], byte[]>> kafkaStreams = messageStreams.get(topic1);
        //创建一个容量为4的线程池
        ExecutorService executor = Executors.newFixedThreadPool(3);
        for (int i=0;i<kafkaStreams.size();i++){
            executor.execute(new KafkaConstomsSimple("消费者" + (i + 1), kafkaStreams .get(i)));
        }


    }
}
