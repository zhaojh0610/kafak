package com.bfxy.kafka.api.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Author: zhaojh
 * @Data: 2020/9/15 16:43
 * @Desc:
 */
public class CustomProducerInterceptor implements ProducerInterceptor {
    private volatile long success = 0;
    private volatile long failure = 0;

    //  发送消息之前的切面拦截
    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        System.out.println("---生产者发送消息前置拦截器---");
        String modifyValue = "prefix-" + record.value();
        return new ProducerRecord<String, String>(record.topic(),
                record.partition(),
                record.timestamp(),
                (String) record.key(),
                modifyValue,
                record.headers());
    }

    //  发送消息之后的切面拦截
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        System.out.println("---生产者发送消息后置拦截器---");
        if (exception == null) {
            success++;
        }else {
            failure++;
        }
    }

    @Override
    public void close() {
        double successRatio = (double) success / (success + failure);
        System.out.println(String.format("生产者关闭，发送消息的成功率为：%s %%", successRatio*100));
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
