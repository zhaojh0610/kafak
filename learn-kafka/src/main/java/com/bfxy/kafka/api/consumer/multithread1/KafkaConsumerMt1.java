package com.bfxy.kafka.api.consumer.multithread1;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @author zhaojh
 * @date 2020/10/30 11:07
 */
public class KafkaConsumerMt1 implements Runnable {

    private KafkaConsumer<String, String> consumer;

    private volatile boolean isRunning = true;

    public KafkaConsumerMt1(Properties properties, String topic) {
        consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        System.out.println("KafkaConsumerMt1 started ");
    }

    @Override
    public void run() {
        try {
            while (isRunning) {
                //  包含所有topic下的所有消息
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                for (TopicPartition topicPartition : consumerRecords.partitions()) {
                    List<ConsumerRecord<String, String>> partitionList = consumerRecords.records(topicPartition);
                    int size = partitionList.size();
                    for (int i = 0; i < size; i++) {
                        ConsumerRecord<String, String> consumerRecord = partitionList.get(i);
                        String message = consumerRecord.value();
                        long offset = consumerRecord.offset();
                        System.out.println("消息内容：" + message + " ,消息便宜量：" + offset);
                    }
                }
            }
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }

    public boolean isRunning() {
        return isRunning;
    }

    public void setRunning(boolean running) {
        isRunning = running;
    }

}
