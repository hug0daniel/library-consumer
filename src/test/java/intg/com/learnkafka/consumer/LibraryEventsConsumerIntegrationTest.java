package com.learnkafka.consumer;


import com.learnkafka.entity.Book;
import com.learnkafka.entity.EventType;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.repository.LibraryEventRepository;
import com.learnkafka.service.ILibraryEventProcessorService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = "library-events",partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventsConsumerIntegrationTest {

    private static final int NUMBER_OF_PARTITIONS = 3;
    private static final Logger log = LoggerFactory.getLogger(LibraryEventsConsumerIntegrationTest.class);

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;
    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
    @Autowired
    private LibraryEventRepository libraryEventRepository;
    @Autowired
    private ObjectMapper objectMapper;

    @MockitoSpyBean
    LibraryEventsConsumer libraryEventsConsumerSpy;
    @MockitoSpyBean
    ILibraryEventProcessorService libraryEventServiceSpy;

    @BeforeEach
    void setUp() {

        for(MessageListenerContainer listenerContainer :kafkaListenerEndpointRegistry.getListenerContainers()) {

            ContainerTestUtils.waitForAssignment(listenerContainer, NUMBER_OF_PARTITIONS);

        }
    }

    @AfterEach
    void tearDown() {
        libraryEventRepository.deleteAll();
    }

    @Test
    void postNewLibraryEvent() throws ExecutionException, InterruptedException {
        //given
        String request = "{\"libraryEventId\": null, \"libraryEventType\": \"NEW\", \"book\": {\"bookName\": \"Kafka Using Spring Boot\", \"bookAuthor\": \"Dilip\"}}";

        kafkaTemplate.sendDefault(request).get();
        final List<String> lalal = new ArrayList<>();
        lalal.add("libraryEventId");
        lalal.add("libraryEventType");
        lalal.removeFirst();
        // when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        //then

        verify(libraryEventsConsumerSpy,  times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy,  times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        List<LibraryEvent> libraryEvents = (List<LibraryEvent>) libraryEventRepository.findAll();
        assertEquals(1, libraryEvents.size());
        libraryEvents.forEach(libraryEvent -> {
            assertNotNull(libraryEvent.getLibraryEventId());
            assertEquals(1,libraryEvent.getBook().getBookId());
        });


    }


    @Test
    void updateLibraryEvent() throws ExecutionException, InterruptedException {
        //given
        String request = "{\"libraryEventId\": null, \"libraryEventType\": \"UPDATE\", \"book\": {\"bookName\": \"Kafka Using Spring Boot\", \"bookAuthor\": \"Dilip\"}}";
        LibraryEvent libraryEvent = objectMapper.readValue(request,LibraryEvent.class);

        libraryEvent.getBook().setLibraryEvent(libraryEvent);

        libraryEventRepository.save(libraryEvent);

        // publish the update event
        Book updatedBook = Book.builder()
                .bookId(1)
                .bookAuthor("Picasso")
                .bookName("Insomnia").build();

        libraryEvent.setLibraryEventType(EventType.UPDATE);
        libraryEvent.setBook(updatedBook);

        String updatedJson = objectMapper.writeValueAsString(libraryEvent);
        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(),updatedJson).get();


        // when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // then
        Optional<LibraryEvent> actualEvent = libraryEventRepository.findById(libraryEvent.getLibraryEventId());

        assertEquals("Insomnia", actualEvent.get().getBook().getBookName());
    }

    @Test
    void updateLibraryEvent_null_libraryEvent() throws ExecutionException, InterruptedException {
        //given
        String request = "{\"libraryEventId\": null, \"libraryEventType\": \"UPDATE\", \"book\": {\"bookName\": \"Kafka Using Spring Boot\", \"bookAuthor\": \"Dilip\"}}";
        kafkaTemplate.sendDefault(request).get();

        // when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);


        // then
        verify(libraryEventsConsumerSpy,  times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy,  times(1)).processLibraryEvent(isA(ConsumerRecord.class));
    }


    @Test
    void updateLibraryEvent_999_libraryEvent() throws ExecutionException, InterruptedException {
        //given
        String request = "{\"libraryEventId\": 999, \"libraryEventType\": \"UPDATE\", \"book\": {\"bookName\": \"Kafka Using Spring Boot\", \"bookAuthor\": \"Dilip\"}}";
        kafkaTemplate.sendDefault(request).get();

        // when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);


        // then
        verify(libraryEventsConsumerSpy,  times(3)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy,  times(3)).processLibraryEvent(isA(ConsumerRecord.class));
    }

}
