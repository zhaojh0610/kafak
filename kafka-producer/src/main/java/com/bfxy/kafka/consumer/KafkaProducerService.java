package com.bfxy.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * @author zhaojh
 * @date 2020/10/30 17:28
 */
@Component
@Slf4j
public class KafkaProducerService {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    public void sendMessage(String topic, Object object) {

        ListenableFuture<SendResult<String, Object>> feFuture = kafkaTemplate.send(topic, object);

        feFuture.addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
            @Override
            public void onFailure(Throwable ex) {
                log.error("消息发送失败：" + ex.getCause());
            }

            @Override
            public void onSuccess(SendResult<String, Object> result) {
                log.info("消息发送成功：" + result.toString());
            }
        });

    }
}
