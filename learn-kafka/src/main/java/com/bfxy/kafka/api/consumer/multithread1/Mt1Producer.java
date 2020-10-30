package com.bfxy.kafka.api.consumer.multithread1;

import com.alibaba.fastjson.JSON;
import com.bfxy.kafka.api.Const;
import com.bfxy.kafka.api.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author zhaojh
 * @date 2020/10/30 10:48
 */
public class Mt1Producer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.120.131:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "mt1-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 10; i++) {
            User user = new User();
            user.setId(i + "");
            user.setName("张三");
            producer.send(new ProducerRecord<String, String>(Const.TOPIC_MT1, JSON.toJSONString(user)));
        }
        producer.close();
    }
}
