package com.etl.migrator.queueConfig;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

import com.etl.migrator.transformer.TransformerLogic;

import lombok.SneakyThrows;

public class MessageConsumer {

    public CountDownLatch latch = new CountDownLatch(3);

    private CountDownLatch partitionLatch = new CountDownLatch(2);

    private CountDownLatch filterLatch = new CountDownLatch(2);


    @KafkaListener(topics = "${message.topic.name}", groupId = "group1", containerFactory = "group1KafkaListenerContainerFactory")
    public void listenGroup1(String message) {
        System.out.println("Received Message from group 'group1': " + message);
        //latch.countDown();
        //CompletableFuture completableFuture = new CompletableFuture();
		//completableFuture.join();
        TransformerLogic trans = new TransformerLogic();
        trans.transformData(message);
    }
    
    @SneakyThrows
    private void sleep() {
        Thread.sleep(5000);
    }

    @KafkaListener(topics = "${message.topic.name}", groupId = "group2", containerFactory = "group2KafkaListenerContainerFactory")
    public void listenGroup2(String message) {
        System.out.println("Received Message in group 'group2': " + message);
        latch.countDown();
    }
}