package com.learnkafka.service;

import com.learnkafka.entity.EventType;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.repository.LibraryEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventService {
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private LibraryEventRepository libraryEventRepository;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) {

        final LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);

        log.info("Event Type: {}", libraryEvent.getLibraryEventType());


        if (libraryEvent.getLibraryEventId() == 999) {
            throw new RecoverableDataAccessException("Temporary Network Issue");
        }

        switch (libraryEvent.getLibraryEventType()) {
            case NEW:
                //save operation
                persist(libraryEvent);
                log.info("Successfully persisted: Event {} with the book {}", libraryEvent.getLibraryEventId(), libraryEvent.getBook());

                break;
            case UPDATE:
                //UPDATE operation

                validate(libraryEvent);
                persist(libraryEvent);
                log.info("Successfully Updated: Event {} with the book {}", libraryEvent.getLibraryEventId(), libraryEvent.getBook());

                break;
            default:
                log.error("Invalid Library event type");
        }
    }

    private void persist(LibraryEvent libraryEvent) {

        if (EventType.NEW.equals(libraryEvent.getLibraryEventType())) {
            libraryEvent.getBook().setBookId(null);  // Ensure's it's null
        }

        libraryEvent.getBook().setLibraryEvent(libraryEvent);

        libraryEventRepository.save(libraryEvent);
    }


    private void validate(LibraryEvent libraryEvent) {

        if (libraryEvent.getLibraryEventId() == null) {
            throw new IllegalArgumentException("Event ID is missing!");
        }


        Optional<LibraryEvent> libraryEventOptional = libraryEventRepository.findById(libraryEvent.getLibraryEventId());

        if (libraryEventOptional.isEmpty()) {
            throw new IllegalArgumentException("Event ID IS NOT VALID");
        }

        log.info("Validation successful");


    }
}
