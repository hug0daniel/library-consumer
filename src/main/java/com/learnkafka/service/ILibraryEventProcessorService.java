package com.learnkafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ILibraryEventProcessorService {
    void processLibraryEvent(ConsumerRecord<Integer,String> consumerRecord);
}
