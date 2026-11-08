package org.example.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Szkolenie Apache Flink, temat T7 - "Jeśli masz czas": agregacja bez okna w SQL i jej strumień
 * zmian w DataStream API. Wynik (tylko dom Gryffindor) idzie przez print() do logów TaskManagera.
 */
public class ChangelogApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql("""
            CREATE TABLE house_events (
                house       STRING,
                `character` STRING,
                score       INT
            ) WITH (
                'connector'                    = 'kafka',
                'topic'                        = 'kafka-input',
                'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9093,kafka-3:9094',
                'properties.group.id'          = 'flink-t7-changelog',
                'scan.startup.mode'            = 'latest-offset',
                'format'                       = 'json'
            )
            """);

        Table totals = tEnv.sqlQuery(
                "SELECT house, COUNT(*) AS how_many, SUM(score) AS sum_score "
                        + "FROM house_events GROUP BY house");

        // TODO: wynik z aktualizacjami jako DataStream<Row> (toDataStream tu nie wystarczy)
        DataStream<Row> changes = null;

        changes.filter(r -> "Gryffindor".equals(r.<String>getFieldAs("house")))
                .map(r -> "ZMIANA " + r.getKind().shortString() + " " + r)
                .print();

        env.execute("changelog-app");
    }
}
