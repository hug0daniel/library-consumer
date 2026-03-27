package com.learnkafka.consumer;

import com.learnkafka.service.ILibraryEventProcessorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LibraryEventsRetryConsumer {

    @Autowired
    private ILibraryEventProcessorService libraryEventService;

    @KafkaListener(topics = "${topics.retry}", groupId = "retry-listener-group")
    public void onMessage(ConsumerRecord<Integer,String> consumerRecord){

        log.info("Incoming Record in RETRY: {}", consumerRecord);
        libraryEventService.processLibraryEvent(consumerRecord);

    }
}
