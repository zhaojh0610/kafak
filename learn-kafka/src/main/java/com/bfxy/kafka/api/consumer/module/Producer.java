package com.bfxy.kafka.api.consumer.module;

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
 */
public class Producer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //  1.配置生产者启动的关键属性参数
        //  1.1 BOOTSTRAP_SERVERS_CONFIG连接kafka集群的服务列表，如果有多个，使用逗号分隔
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.120.131:9092");
        //  1.2 CLIENT_ID_CONFIG这个属性的目的是标记kafkaclient的ID,可以没有
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, Const.TOPIC_MODULE);
        //  1.3 对kafka的key和value做序列化处理，因为kafka只能以二进制的形式接受消息
        //  key：是kafka用于消息投递计算具体投递到对应主题的哪一个partition而需要的
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //  value：实际发送消息的内容
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //  2.创建kafka生产者对象，传递properties属性参数集合
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        //  3.构造消息内容
        for (int i = 0; i < 10; i++) {
            User user = new User("00" + i, "张三");
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(Const.TOPIC_MODULE, JSON.toJSONString(user));
            //  4.发送消息
            producer.send(record);
        }
        //  5.关闭
        producer.close();
    }
}
