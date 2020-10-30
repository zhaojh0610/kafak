package com.bfxy.kafka;

import com.bfxy.kafka.consumer.KafkaProducerService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ApplicationTests {

    @Autowired
    private KafkaProducerService kafkaProducerService;
    @Test
    public void sendMessage() throws InterruptedException {
        String topic = "topic02";
        for (int i = 0; i < 2000; i++) {
            kafkaProducerService.sendMessage(topic, "hello kafka " + i);
            Thread.sleep(50);
        }

        Thread.sleep(Integer.MAX_VALUE);
    }

}
