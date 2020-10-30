package com.bfxy.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * @author zhaojh
 * @date 2020/10/30 17:28
 */
@Component
@Slf4j
public class KafkaConsumerService {

    @KafkaListener(groupId = "group02", topics = "topic02")
    public void onMessage(ConsumerRecord<String, Object> record, Acknowledgment acknowledgment, Consumer<?, ?> consumer) {
        log.info("消费端收到的消息：" + record.value());
        //  手工签收机制
        acknowledgment.acknowledge();
    }
}
