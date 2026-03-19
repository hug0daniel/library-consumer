package com.learnkafka.service;

import com.learnkafka.entity.EventType;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.repository.LibraryEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

@Service
@Slf4j
public class LibraryEventService {
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private LibraryEventRepository libraryEventRepository;
    public void processLibraryEvent(ConsumerRecord<Integer,String> consumerRecord) {

        final LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);

       log.info("LibraryEvent: {}", libraryEvent);

       switch (libraryEvent.getLibraryEventType()) {
           case NEW:
           //save operation
           persist(libraryEvent);
           break;
           case UPDATE:
               //UPDATE operation
           break;
           default: log.error("Invalid Library event type");
       }
    }

    private void persist(LibraryEvent libraryEvent) {

        if (EventType.NEW.equals(libraryEvent.getLibraryEventType())) {
            libraryEvent.getBook().setBookId(null);  // Ensure's it's null
        }

        libraryEvent.getBook().setLibraryEvent(libraryEvent);

        libraryEventRepository.save(libraryEvent);

        log.info("Successfully persisted the library event: {} with the book {}", libraryEvent, libraryEvent.getBook());
    }
}
