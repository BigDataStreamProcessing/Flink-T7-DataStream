package org.example.flink;

import java.time.Instant;

/**
 * Zdarzenie HouseStats - POJO (publiczne pola, konstruktor bez argumentów).
 *
 * Dwa pola czasu, bo zdarzenie może przyjść dwiema drogami:
 * - ts - tekst z datą z pola "ts" w JSON; wypełnia go JsonDeserializationSchema, gdy czytamy
 *   temat przez KafkaSource (TriggersApp),
 * - eventTime - czas zdarzenia ze znacznika rekordu Kafki; wypełnia go toDataStream, gdy źródłem
 *   jest tabela house_events z kolumną METADATA FROM 'timestamp' (LateApp). W TriggersApp zostaje
 *   null - tam czas zdarzenia i tak niesie znacznik rekordu, a nie pole obiektu.
 */
public class HouseEvent {
    public String house;
    public String character;
    public int score;
    public String ts;
    public Instant eventTime;

    public HouseEvent() {}

    @Override
    public String toString() {
        return "HouseEvent{" + house + ", " + character + ", " + score + ", ts=" + ts
                + ", eventTime=" + eventTime + "}";
    }
}
