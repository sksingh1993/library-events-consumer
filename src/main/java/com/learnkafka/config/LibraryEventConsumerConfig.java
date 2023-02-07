package com.learnkafka.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventConsumerConfig {
    /**
     * This use for many custom configuration
     * Manual offset commit
     * Concurrency
     * CommonErrorHandler
     */

    @Bean
    ConcurrentKafkaListenerContainerFactory<?,?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory){
        ConcurrentKafkaListenerContainerFactory<Object,Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory,kafkaConsumerFactory);
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(errorHandler());
        //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        return factory;
    }

    public DefaultErrorHandler errorHandler(){

        //Default attemps try 10 times now it will attempts 3 times 1 attemp original and 2 re try
        FixedBackOff fixedBackOff = new FixedBackOff(1000L, 2);

        DefaultErrorHandler defaultErrorHandler = new DefaultErrorHandler(fixedBackOff);

        //Listen to each re try attemps

        defaultErrorHandler.setRetryListeners((consumerRecord, e, i) -> {
            log.info("Failed Record in Retry Listener, Exception : {} , deleveryAttemp : {} ", e.getMessage(), i);
        });

        return defaultErrorHandler;
    }
}
