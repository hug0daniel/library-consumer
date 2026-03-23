package com.learnkafka.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.kafka.autoconfigure.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import java.util.List;

@Slf4j
@Configuration
@EnableKafka
public class LibraryEventsConsumerConfig {

    private DefaultErrorHandler errorHandler(){

        final List<Class<IllegalArgumentException>> exceptionIgnoreList = List.of(IllegalArgumentException.class);

        //final FixedBackOff fixedBackOff = new FixedBackOff(1000L,2);

        final ExponentialBackOffWithMaxRetries exponentialBackOff = new ExponentialBackOffWithMaxRetries(2);
        exponentialBackOff.setInitialInterval(1_000L);
        exponentialBackOff.setMultiplier(2.0);
        exponentialBackOff.setMaxInterval(2_000L);
        final DefaultErrorHandler errorHandler = new DefaultErrorHandler(exponentialBackOff);

        exceptionIgnoreList.forEach(errorHandler::addNotRetryableExceptions);

        errorHandler
                .setRetryListeners( ((record, ex, deliveryAttempt) -> {
                    assert ex != null;
                    log.info("Failed Record in RETRY LISTENER, Exception: {}, deliveryAttempt: {} ",ex.getMessage(),deliveryAttempt);
                }));

        return errorHandler;
    }
    @Bean
    ConcurrentKafkaListenerContainerFactory<?,?> kafkaListenerContainerFactory(ConcurrentKafkaListenerContainerFactoryConfigurer configurer, ConsumerFactory<Object,Object> kafkaconsumerFactory){
        ConcurrentKafkaListenerContainerFactory<Object,Object> factory = new ConcurrentKafkaListenerContainerFactory<>();

        configurer.configure(factory,kafkaconsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setConcurrency(3); // option to scale consumer - recommended if application not running on cloud env
        factory.setCommonErrorHandler(errorHandler());
        return factory;
    }
}
