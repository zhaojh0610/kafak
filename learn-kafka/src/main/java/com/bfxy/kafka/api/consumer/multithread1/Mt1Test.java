package com.bfxy.kafka.api.consumer.multithread1;

import com.bfxy.kafka.api.Const;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author zhaojh
 * @date 2020/10/30 10:44
 */
public class Mt1Test {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.120.131:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "topic-group");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,"30000" );
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        String topic = Const.TOPIC_MT1;

        //  coreSize
        int coreSize = 5;
        ExecutorService executor;
        ExecutorService executorService = Executors.newFixedThreadPool(coreSize);
        for (int i = 0; i < coreSize; i++) {
            executorService.execute(new KafkaConsumerMt1(props, topic));
        }


    }
}
