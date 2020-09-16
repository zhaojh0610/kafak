package com.bfxy.kafka.api.consumer.module;

import com.bfxy.kafka.api.Const;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @author zhaojh
 */
@Slf4j
public class Consumer1 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //  1.配置消费者
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.120.131:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //  非常重要的属性配置：与我们消费者订阅组有关系
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "module-group-id-1");
        //  常规属性：会话连接超时时间
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        //  消费者提交offset：自动提交&手工提交，默认是自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 9000);
        //  2.创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        //  订阅你感兴趣的主题
        consumer.subscribe(Collections.singletonList(Const.TOPIC_MODULE));
        System.out.println(String.format("consumer1 consumer started..."));
        try {
            //  采用拉取消息的方式消费数据
            while (true) {
                //  等待多久拉取一次消息
                //  拉取topic_quickstart主题里面所有的消息
                //  topic和partition是一对多的关系，一个topic可以有多个partition
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                //  因为消息是在partition中存储的，所以需要遍历partition集合
                for (TopicPartition topicPartition : records.partitions()) {
                    //  获取topicPartition对应的主题名称
                    String topic = topicPartition.topic();
                    //  通过topicPartition获取指定的数据集合，获取到的就是当前topicPartition下面的所有消息
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(topicPartition);
                    //  获取当前partition消息条数
                    int size = partitionRecords.size();
                    System.out.println(String.format("---获取topic:%s,分区位置：%s,消息总数：%s", topic, topicPartition.partition(), size));
                    System.out.println(String.format("---获取topic:{},分区位置：{},消息总数：{}", topic, topicPartition.partition(), size));
                    for (int i = 0; i < size; i++) {
                        ConsumerRecord<String, String> consumerRecord = partitionRecords.get(i);
                        //  实际的数据内容
                        String value = consumerRecord.value();
                        //  当前获取的消息偏移量
                        long offset = consumerRecord.offset();
                        //  ISR ：High Watermark,如果要提交的话，比如提交当前消息的offset+1
                        //  表示下一次从什么位置（offset）拉取消息
                        long commitOffser = offset + 1;
                        System.out.println(String.format("---获取实际消息value:%s,消息offset：%s,提交offset：%s", value, offset, commitOffser));
                    }
                }
            }
        } finally {
            consumer.close();
        }

    }
}
