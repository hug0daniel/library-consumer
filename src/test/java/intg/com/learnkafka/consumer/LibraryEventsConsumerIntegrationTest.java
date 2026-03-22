package com.learnkafka.consumer;


import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.repository.LibraryEventRepository;
import com.learnkafka.service.LibraryEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.util.List;
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

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;
    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Autowired
    private LibraryEventRepository libraryEventRepository; // ← this + RestTestClient replaces TestRestTemplate

    @MockitoSpyBean
    LibraryEventsConsumer libraryEventsConsumerSpy;
    @MockitoSpyBean
    LibraryEventService libraryEventServiceSpy;

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
    void publishNewLibraryEvent() throws ExecutionException, InterruptedException {
        //given
        String request = "{\"libraryEventId\": null, \"libraryEventType\": \"NEW\", \"book\": {\"bookName\": \"Kafka Using Spring Boot\", \"bookAuthor\": \"Dilip\"}}";

        kafkaTemplate.sendDefault(request).get();

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

}
