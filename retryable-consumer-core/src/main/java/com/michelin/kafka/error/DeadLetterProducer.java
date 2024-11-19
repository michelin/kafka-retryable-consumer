package com.michelin.kafka.error;

import com.michelin.kafka.configuration.DeadLetterProducerConfiguration;
import com.michelin.kafka.configuration.RetryableConsumerConfiguration;
import com.michelin.kafka.avro.GenericErrorModel;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Dlq producer
 */
@Slf4j
@AllArgsConstructor
public class DeadLetterProducer {

    /**
     * Kafka producer
     * - Key is a UUID string
     * - Value is a GenericErrorModel object (see GenericErrorModel avro doc)
     */
    private final Producer<String, GenericErrorModel> kafkaProducer;
    private final String dlTopic;

    /**
     * Constructor
     */
    public DeadLetterProducer(DeadLetterProducerConfiguration configuration) {
        Properties dlProducerProperties = configuration.getProperties();
        kafkaProducer = new KafkaProducer<>(dlProducerProperties);
        this.dlTopic = configuration.getTopic();
    }

    public DeadLetterProducer(DeadLetterProducerConfiguration configuration, Producer<String, GenericErrorModel> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
        this.dlTopic = configuration.getTopic();
    }

    /**
     * Send message to dlq by kafka
     *
     * @param key         message key
     * @param errorObject object wrapping the error to send to dlq
     */
    public void send(String key, GenericErrorModel errorObject) {
        log.info("Sending error to DLQ topic = {}", dlTopic);
        ProducerRecord<String, GenericErrorModel> rec =
                new ProducerRecord<>(
                        dlTopic,
                        key,
                        errorObject
                );



        this.kafkaProducer.send(rec, (recordMetadata, e) -> {
            if (e != null) {
                log.error(e.getMessage());
            } else {
                String errorRecordKey = "unknown key";
                if(rec.value().getKey() != null) {
                    errorRecordKey =  rec.value().getKey();
                } else if (rec.value().getByteKey() != null) {
                    errorRecordKey = rec.value().getByteKey().toString();
                }

                log.info(
                        "Error management about issue with event key '{}' " +
                                "has been successfully traced in DLQ topic " +
                                "= {} partition = {} offset = {}",
                        errorRecordKey,
                        recordMetadata.topic(),
                        recordMetadata.partition(),
                        recordMetadata.offset()
                );
            }
        }); //Remember that the actual sending of data to kafka cluster is done asynchronously in a separated thread
    }
}
