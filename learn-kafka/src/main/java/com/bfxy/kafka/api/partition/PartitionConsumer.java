package com.bfxy.kafka.api.partition;

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
public class PartitionConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.120.131:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, Const.TOPIC_PARTITION);
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 9000);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singletonList(Const.TOPIC_PARTITION));
        System.out.println(String.format("partition consumer started..."));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (TopicPartition topicPartition : records.partitions()) {
                    String topic = topicPartition.topic();
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(topicPartition);
                    int size = partitionRecords.size();
                    System.out.println(String.format("---获取topic:%s,分区位置：%s,消息总数：%s", topic, topicPartition.partition(), size));
                    System.out.println(String.format("---获取topic:{},分区位置：{},消息总数：{}", topic, topicPartition.partition(), size));
                    for (int i = 0; i < size; i++) {
                        ConsumerRecord<String, String> consumerRecord = partitionRecords.get(i);
                        String value = consumerRecord.value();
                        long offset = consumerRecord.offset();
                        long commitOffer = offset + 1;
                        System.out.println(String.format("---获取实际消息value:%s,消息offset：%s,提交offset：%s", value, offset, commitOffer));
                    }
                }
            }
        } finally {
            consumer.close();
        }

    }
}
