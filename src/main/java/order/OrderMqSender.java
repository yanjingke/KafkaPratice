package order;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.UUID;

public class OrderMqSender {
    public static void main(String[] args) {
        String TOPIC="OrderMq";
        Properties  properties=new Properties();
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        //设置kafka broker对应的主机
        properties.put("metadata.broker.list","hadoop:9092,hadoop2:9092,hadoop3:9092");
        properties.put("request.required.acks","1");
        properties.put("partitioner.class","MyPatition");
        Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(properties));
        /**
         * 5、调用producer的send方法发送数据
         * 注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发
         */
        for (int mesageNo=1;mesageNo<100000;mesageNo++){
            //    TOPIC,mesageNo+"" ,"appid"+UUID.randomUUID()+"itcast"

            producer.send(new KeyedMessage<String, String>(TOPIC, mesageNo + "", new OrderInfo().random()));


        }
    }
}
