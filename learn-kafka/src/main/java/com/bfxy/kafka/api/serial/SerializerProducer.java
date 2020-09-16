package com.bfxy.kafka.api.serial;

import com.bfxy.kafka.api.Const;
import com.bfxy.kafka.api.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author zhaojh
 */
public class SerializerProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.120.131:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "serial-producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserSerializer.class.getName());
        KafkaProducer<String, User> producer = new KafkaProducer<String, User>(properties);
        for (int i = 0; i < 10; i++) {
            User user = new User("00" + i, "张三");
            ProducerRecord<String, User> record = new ProducerRecord<String, User>(Const.TOPIC_SERIAL, user);
            producer.send(record);
        }
        producer.close();
    }
}
