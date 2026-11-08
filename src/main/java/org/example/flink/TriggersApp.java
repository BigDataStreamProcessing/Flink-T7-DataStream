package org.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;

import org.example.flink.connectors.KafkaSinks;
import org.example.flink.functions.CountSum;
import org.example.flink.functions.Diagnostics;
import org.example.flink.model.Emission;
import org.example.flink.model.HouseEvent;
import org.example.flink.triggers.EarlyOnTimeLate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Szkolenie Apache Flink, temat T7 - wyzwalacze w liczbach (część A warsztatu w7).
 *
 * Ten sam strumień zdarzeń, te same okna stałe 1 min w czasie zdarzenia (watermark z zapasem
 * 40 s - więcej niż opóźnienie generatora) i pięć gałęzi różniących się tylko wyzwalaczem.
 * Każda emisja każdej gałęzi trafia jako JSON do tematu house-emissions (klasa Emission z polem
 * variant), gdzie policzymy je w SQL Client.
 */
public class TriggersApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ===== Źródło i watermark (jak na wykładzie) =====
        KafkaSource<HouseEvent> kafkaSource = KafkaSource.<HouseEvent>builder()
                .setBootstrapServers(KafkaSinks.BOOTSTRAP)
                .setTopics("kafka-input")
                .setGroupId("flink-t7-triggers")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(HouseEvent.class))
                .build();

        WatermarkStrategy<HouseEvent> watermarkStrategy = WatermarkStrategy
                .<HouseEvent>forBoundedOutOfOrderness(Duration.ofSeconds(40))
                .withIdleness(Duration.ofMinutes(1));
        // bez withTimestampAssigner: znacznik rekordu Kafki = czas zdarzenia

        DataStream<HouseEvent> events =
                env.fromSource(kafkaSource, watermarkStrategy, "house-events");

        KeyedStream<HouseEvent, String> byHouse = events.keyBy(e -> e.house);
        TumblingEventTimeWindows minute = TumblingEventTimeWindows.of(Duration.ofMinutes(1));

        // ===== Gałęzie: to samo okno, różne wyzwalacze =====
        // Każda gałąź zaczyna od byHouse.window(minute) - nowy WindowedStream z własnym
        // wyzwalaczem - i kończy się tym samym: aggregate(new CountSum(), new Diagnostics(...)).
        List<DataStream<Emission>> branches = new ArrayList<>();

        // wzorzec: wyzwalacz domyślny okna czasu zdarzenia
        branches.add(byHouse.window(minute)
                .aggregate(new CountSum(), new Diagnostics("default"))
                .name("okno-default"));

        // TODO 1: gałąź "count" - CountTrigger co 50 zdarzeń (nazwa operatora "okno-count")

        // TODO 2: gałąź "continuous" - ContinuousEventTimeTrigger co 15 s (nazwa "okno-continuous")

        // TODO 3: gałąź "purging" - ten sam wyzwalacz co w TODO 2 opakowany w PurgingTrigger
        //         (nazwa "okno-purging")

        // TODO 4: gałąź "eotl" - EarlyOnTimeLate z wczesnymi wynikami co 10 s (nazwa "okno-eotl");
        //         najpierw uzupełnij klasę EarlyOnTimeLate (TODO 4a-4c)

        // ===== Ujście: wszystkie emisje do jednego tematu =====
        DataStream<Emission> emissions = branches.get(0);
        for (int i = 1; i < branches.size(); i++) {
            emissions = emissions.union(branches.get(i));
        }
        emissions.sinkTo(KafkaSinks.<Emission>json("house-emissions")).name("house-emissions");

        env.execute("triggers-app");
    }
}
