package org.example.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.OutputTag;

import org.example.flink.connectors.KafkaSinks;
import org.example.flink.functions.CountSum;
import org.example.flink.functions.Diagnostics;
import org.example.flink.model.Emission;
import org.example.flink.model.HouseEvent;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Szkolenie Apache Flink, temat T7 - spóźnione zdarzenia i kanapka SQL - DataStream - SQL
 * (część B warsztatu w7).
 *
 * Źródło: tabela house_events w SQL z celowo MAŁYM zapasem watermarku (5 s przy opóźnieniach
 * generatora do 30 s), więc część zdarzeń będzie spóźniona. Okno 1 min w DataStream API czeka
 * na spóźnione 10 s, a jeszcze późniejsze kieruje do wyjścia bocznego (temat house-late).
 * Kolejne wersje wyniku wracają do SQL jako strumień zmian i trafiają przez upsert do tabeli
 * housestats w MySQL (ta sama co w warsztacie T5).
 */
public class LateApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // ===== Źródło w SQL: pole ts z JSON i czas zdarzenia ze znacznika rekordu Kafki =====
        tEnv.executeSql("""
            CREATE TABLE house_events (
                house       STRING,
                `character` STRING,
                score       INT,
                ts          STRING,
                eventTime   TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
                WATERMARK FOR eventTime AS eventTime - INTERVAL '5' SECOND
            ) WITH (
                'connector'                    = 'kafka',
                'topic'                        = 'kafka-input',
                'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9093,kafka-3:9094',
                'properties.group.id'          = 'flink-t7-late',
                'scan.startup.mode'            = 'earliest-offset',
                'scan.watermark.idle-timeout'  = '1 min',
                'format'                       = 'json'
            )
            """);

        // ===== Ujście w SQL: tabela housestats w MySQL (klucz główny jak w T5) =====
        tEnv.executeSql("""
            CREATE TABLE housestats_sink (
                house        STRING,
                how_many     BIGINT,
                sum_score    BIGINT,
                window_start TIMESTAMP(3),
                window_end   TIMESTAMP(3),
                PRIMARY KEY (house, window_start, window_end) NOT ENFORCED
            ) WITH (
                'connector'  = 'jdbc',
                'url'        = 'jdbc:mysql://mysql:3306/streamdb',
                'table-name' = 'housestats',
                'username'   = 'streamuser',
                'password'   = 'stream'
            )
            """);

        // ===== 1. Z tabeli do strumienia =====
        // TODO 1: tabela house_events jako DataStream<HouseEvent> (toDataStream z klasą docelową)
        DataStream<HouseEvent> events = null;

        // ===== 2. Okno, które czeka na spóźnione, i wyjście boczne =====
        final OutputTag<HouseEvent> late = new OutputTag<>("late") {};

        // TODO 2: okno stałe 1 min po domach; spóźnione czekają 10 s (allowedLateness),
        //         jeszcze późniejsze idą do wyjścia bocznego "late" (sideOutputLateData);
        //         wynik: aggregate(new CountSum(), new Diagnostics("late-app")),
        //         nazwa operatora "okno-late-app"
        SingleOutputStreamOperator<Emission> stats = null;
        // TODO 2: strumień spóźnionych z wyjścia bocznego (getSideOutput)
        DataStream<HouseEvent> lateEvents = null;

        // gotowe: każda emisja do house-emissions (jak w części A), spóźnione do house-late
        stats.sinkTo(KafkaSinks.<Emission>json("house-emissions")).name("house-emissions");
        lateEvents.sinkTo(KafkaSinks.<HouseEvent>json("house-late")).name("house-late");

        // ===== 3. Kolejne wersje wyniku jako strumień zmian =====
        // TODO 3: każdą emisję zamień na Row rodzaju UPDATE_AFTER z polami
        //         house, how_many, sum_score, window_start, window_end
        //         (granice okna przez toLocal(...)) i opisz typ przez .returns(Types.ROW_NAMED(...))
        DataStream<Row> versions = null;

        // ===== 4. Ze strumienia zmian do tabeli =====
        // TODO 4: fromChangelogStream z trybem upsert; w Schema zadeklaruj wszystkie kolumny
        //         z typami ujścia (kolumny klucza NOT NULL) i klucz główny
        //         (house, window_start, window_end)
        Table result = null;

        // ===== 5. Jedno zadanie: część SQL dołączona do grafu DataStream =====
        // TODO 5: StreamStatementSet z result.insertInto("housestats_sink"),
        //         attachAsDataStream() - a potem jedno env.execute(...)

        env.execute("late-app");
    }

    /** Granica okna (epoch ms) jako TIMESTAMP bez strefy - w UTC, jak w tabeli z T5. */
    static LocalDateTime toLocal(long epochMillis) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC);
    }
}
