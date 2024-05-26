package com.learnkafkastreams.config;

import com.learnkafkastreams.exceptionhandler.StreamsProcessorCustomErrorHandler;
import com.learnkafkastreams.topology.OrdersTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler;

@Configuration
@Slf4j
public class OrdersStreamsConfiguration {
    //KafkaStreamsDefaultConfiguration -> Class Responsible for configuring the KafkaStreams in SpringBoot
    private final KafkaProperties kafkaProperties;

    private final KafkaTemplate<String, String> kafkaTemplate;

  public OrdersStreamsConfiguration(KafkaProperties kafkaProperties,
                                    KafkaTemplate<String, String> kafkaTemplate) {
    this.kafkaProperties = kafkaProperties;
    this.kafkaTemplate = kafkaTemplate;
  }

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamConfig(ObjectProvider<SslBundles> sslBundles) {
        var streamProperties = kafkaProperties
                .buildStreamsProperties(sslBundles.getIfAvailable());

        streamProperties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, RecoveringDeserializationExceptionHandler.class);
        streamProperties.put(RecoveringDeserializationExceptionHandler.KSTREAM_DESERIALIZATION_RECOVERER, consumerRecordRecoverer);

        return new KafkaStreamsConfiguration(streamProperties);
    }

    @Bean
    public StreamsBuilderFactoryBeanConfigurer streamsBuilderFactoryBeanConfigurer(){
        log.info("Inside streamsBuilderFactoryBeanConfigurer");
        return factoryBean ->
            factoryBean.setStreamsUncaughtExceptionHandler(new StreamsProcessorCustomErrorHandler());
    }


    public DeadLetterPublishingRecoverer recoverer() {
        return new DeadLetterPublishingRecoverer(kafkaTemplate,
                (consumerRecord, ex) -> {
                    log.error("Exception in Deserializing the consumerRecord : {} and the consumerRecord is : {}", ex.getMessage(),consumerRecord,  ex);
                    return new TopicPartition("recovererDLQ", consumerRecord.partition());
                });
    }


    ConsumerRecordRecoverer consumerRecordRecoverer = (consumerRecord, exception) ->
            log.error("Exception is : {} Failed Record : {} ", exception, consumerRecord);

    @Bean
    public NewTopic topicBuilder() {
        return TopicBuilder.name(OrdersTopology.ORDERS)
                .partitions(2)
                .replicas(1)
                .build();

    }

    @Bean
    public NewTopic storeTopicBuilder() {
        return TopicBuilder.name(OrdersTopology.STORES)
                .partitions(2)
                .replicas(1)
                .build();

    }
}
