package org.example.flink.connectors;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.json.JsonSerializationSchema;

/** Ujście Kafka zapisujące obiekty jako JSON - jak na wykładzie, tylko z tematem w parametrze. */
public final class KafkaSinks {

    public static final String BOOTSTRAP = "kafka-1:9092,kafka-2:9093,kafka-3:9094";

    private KafkaSinks() {}

    public static <T> KafkaSink<T> json(String topic) {
        return KafkaSink.<T>builder()
                .setBootstrapServers(BOOTSTRAP)
                .setRecordSerializer(KafkaRecordSerializationSchema.<T>builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new JsonSerializationSchema<T>())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}
