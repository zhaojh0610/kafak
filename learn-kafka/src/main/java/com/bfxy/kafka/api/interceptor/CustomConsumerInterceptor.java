package com.bfxy.kafka.api.interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Map;

/**
 * @Author: zhaojh
 * @Data: 2020/9/15 16:45
 * @Desc:
 */
public class CustomConsumerInterceptor implements ConsumerInterceptor {
    @Override
    public ConsumerRecords onConsume(ConsumerRecords records) {
        return null;
    }

    @Override
    public void onCommit(Map offsets) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
