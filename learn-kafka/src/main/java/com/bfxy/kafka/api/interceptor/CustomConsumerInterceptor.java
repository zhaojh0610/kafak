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

    //  onConsume:  消费者接到消息处理之前的拦截器
    @Override
    public ConsumerRecords onConsume(ConsumerRecords records) {
        System.out.println("---消费者前置处理器，接受消息 ---");
        return records;
    }

    @Override
    public void onCommit(Map offsets) {
        offsets.forEach((tp, offset) -> {
            System.out.println("消费者处理完成，" + "分区：" + tp + ", 偏移量：" + offset);
        });
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
