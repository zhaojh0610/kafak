package com.bfxy.kafka.api.consumer.core;

import com.bfxy.kafka.api.Const;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author zhaojh
 * @date 2020/9/27 15:43
 */
public class CommitConsumer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.120.131:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, Const.TOPIC_CORE);
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        //  使用手工提交方式
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 9000);

        //  消费者默认每次拉取的位置：从什么位置开始拉取消息
        //  AUTO_OFFSET_RESET_CONFIG 有三种方式："latest","earliest","none"
        //  none
        //  latest  从一个分区的最后提交offset开始拉取消息
        //  earliset    从最开始的起始位置拉取消息0
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        //  对于consumer消息的订阅subscribe方法，可以订阅一个或者多个topic
        consumer.subscribe(Collections.singletonList(Const.TOPIC_CORE));
        System.out.println(String.format("core consumer started..."));
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
                        //  一个一个partition同步提交
//                        consumer.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(commitOffer)));
                        //  一个一个partition异步提交
                        consumer.commitAsync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(commitOffer)),
                                new OffsetCommitCallback() {
                                    @Override
                                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                                        if (exception != null) {
                                            System.out.println("提交失败！");
                                        }
                                        System.out.println("异步提交成功：" + offsets);
                                    }
                                });
                    }
                }
                /*
                //  整体提交：同步方式
//                consumer.commitSync();
                //  整体提交：异步方式
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        if (exception != null) {
                            System.out.println("提交失败。。。");
                        }
                        System.out.println("异步提交成功：" + offsets);
                    }
                });
                */
            }
        } finally {
            consumer.close();
        }

    }
}
