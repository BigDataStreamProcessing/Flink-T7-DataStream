package org.example.flink.connectors;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.util.jackson.JacksonMapperFactory;

/**
 * Ujście Kafka zapisujące obiekty jako JSON - jak na wykładzie, tylko z tematem w parametrze.
 *
 * JsonSerializationSchema bez parametrów używa gołego ObjectMappera, który nie zna typów
 * java.time (Instant w HouseEvent.eventTime kończy się wyjątkiem "Could not serialize value").
 * Dlatego podajemy ten sam mapper, którego domyślnie używa JsonDeserializationSchema
 * (JacksonMapperFactory: moduły JavaTimeModule i Jdk8Module, daty jako tekst ISO-8601).
 */
public final class KafkaSinks {

    public static final String BOOTSTRAP = "kafka-1:9092,kafka-2:9093,kafka-3:9094";

    private KafkaSinks() {}

    public static <T> KafkaSink<T> json(String topic) {
        return KafkaSink.<T>builder()
                .setBootstrapServers(BOOTSTRAP)
                .setRecordSerializer(KafkaRecordSerializationSchema.<T>builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(
                                new JsonSerializationSchema<T>(JacksonMapperFactory::createObjectMapper))
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}
