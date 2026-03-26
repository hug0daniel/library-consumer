package com.learnkafka.consumer;

import com.learnkafka.service.ILibraryEventProcessorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LibraryEventsConsumer {

    @Autowired
    private ILibraryEventProcessorService libraryEventService;

    @KafkaListener(topics = "library-events")
    public void onMessage(ConsumerRecord<Integer,String> consumerRecord){

        log.info("Incoming Record: {}", consumerRecord);
        libraryEventService.processLibraryEvent(consumerRecord);

    }
}
