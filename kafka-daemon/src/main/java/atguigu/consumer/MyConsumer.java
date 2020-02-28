package atguigu.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class MyConsumer {
    public static void main(String[] args) {
        //1.创建消费者配置信息
        Properties properties = new Properties();
        //2.修改配置信息
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        //反序列化的类
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        //消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"bigdata");


        //3.创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        //订阅主题
        consumer.subscribe(Collections.singletonList("aaa"));
        while (true) {
            //获取数据
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            //解析遍历数据
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.key() + "------" + consumerRecord.value());
            }
        }

    }



}
