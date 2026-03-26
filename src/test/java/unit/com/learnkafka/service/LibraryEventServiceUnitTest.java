package com.learnkafka.service;

import com.learnkafka.entity.Book;
import com.learnkafka.entity.EventType;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.repository.LibraryEventRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.RecoverableDataAccessException;
import tools.jackson.databind.ObjectMapper;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LibraryEventServiceUnitTest {

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private LibraryEventRepository libraryEventRepository;

    @InjectMocks
    private LibraryEventProcessorService libraryEventService;

    private ConsumerRecord<Integer, String> buildConsumerRecord(String json) {
        return new ConsumerRecord<>("library-events", 0, 0L, 123, json);
    }

    @Test
    void processLibraryEvent_newEvent_persistsSuccessfully() throws Exception {
        // given
        Book book = Book.builder()
                .bookId(456)
                .bookName("Kafka Using Spring Boot")
                .bookAuthor("Dilip")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .libraryEventType(EventType.NEW)
                .book(book)
                .build();

        String json = "{\"dummy\": \"json\"}";
        ConsumerRecord<Integer, String> consumerRecord = buildConsumerRecord(json);

        when(objectMapper.readValue(json, LibraryEvent.class)).thenReturn(libraryEvent);

        // when
        libraryEventService.processLibraryEvent(consumerRecord);

        // then
        ArgumentCaptor<LibraryEvent> captor = ArgumentCaptor.forClass(LibraryEvent.class);
        verify(libraryEventRepository, times(1)).save(captor.capture());

        LibraryEvent saved = captor.getValue();
        assertEquals(EventType.NEW, saved.getLibraryEventType());
        assertNull(saved.getBook().getBookId()); // persist must set null
        assertEquals(saved, saved.getBook().getLibraryEvent());
    }



    @Test
    void processLibraryEvent_updateEvent_validEvent_updatesSuccessfully() throws Exception {
        // given
        Book book = Book.builder()
                .bookId(456)
                .bookName("Kafka Using Spring Boot")
                .bookAuthor("Dilip")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(1)
                .libraryEventType(EventType.UPDATE)
                .book(book)
                .build();

        String json = "{\"dummy\": \"json\"}";
        ConsumerRecord<Integer, String> consumerRecord = buildConsumerRecord(json);

        when(objectMapper.readValue(json, LibraryEvent.class)).thenReturn(libraryEvent);
        when(libraryEventRepository.findById(1)).thenReturn(Optional.of(libraryEvent));

        // when
        libraryEventService.processLibraryEvent(consumerRecord);

        // then
        verify(libraryEventRepository, times(1)).findById(1);
        verify(libraryEventRepository, times(1)).save(libraryEvent);
        assertEquals(libraryEvent, libraryEvent.getBook().getLibraryEvent());
    }


    @Test
    void processLibraryEvent_updateEvent_nullId_throwsException() throws Exception {
        // given
        Book book = Book.builder()
                .bookId(456)
                .bookName("Kafka Using Spring Boot")
                .bookAuthor("Dilip")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .libraryEventType(EventType.UPDATE)
                .book(book)
                .build();

        String json = "{\"dummy\": \"json\"}";
        ConsumerRecord<Integer, String> consumerRecord = buildConsumerRecord(json);

        when(objectMapper.readValue(json, LibraryEvent.class)).thenReturn(libraryEvent);

        // when / then
        assertThrows(IllegalArgumentException.class,
                () -> libraryEventService.processLibraryEvent(consumerRecord));

        verify(libraryEventRepository, never()).save(any());
    }


    @Test
    void processLibraryEvent_updateEvent_invalidId_throwsException() throws Exception {
        // given
        Book book = Book.builder()
                .bookId(456)
                .bookName("Kafka Using Spring Boot")
                .bookAuthor("Dilip")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(1)
                .libraryEventType(EventType.UPDATE)
                .book(book)
                .build();

        String json = "{\"dummy\": \"json\"}";
        ConsumerRecord<Integer, String> consumerRecord = buildConsumerRecord(json);

        when(objectMapper.readValue(json, LibraryEvent.class)).thenReturn(libraryEvent);
        when(libraryEventRepository.findById(1)).thenReturn(Optional.empty());

        // when / then
        assertThrows(IllegalArgumentException.class,
                () -> libraryEventService.processLibraryEvent(consumerRecord));

        verify(libraryEventRepository, times(1)).findById(1);
        verify(libraryEventRepository, never()).save(any());
    }


    @Test
    void processLibraryEvent_id999_throwsRecoverableDataAccessException() throws Exception {
        // given
        Book book = Book.builder()
                .bookId(456)
                .bookName("Kafka Using Spring Boot")
                .bookAuthor("Dilip")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(999)
                .libraryEventType(EventType.NEW)
                .book(book)
                .build();

        String json = "{\"dummy\": \"json\"}";
        ConsumerRecord<Integer, String> consumerRecord = buildConsumerRecord(json);

        when(objectMapper.readValue(json, LibraryEvent.class)).thenReturn(libraryEvent);

        // when / then
        assertThrows(RecoverableDataAccessException.class,
                () -> libraryEventService.processLibraryEvent(consumerRecord));

        verify(libraryEventRepository, never()).save(any());
    }
}