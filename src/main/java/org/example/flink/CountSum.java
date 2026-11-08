package org.example.flink;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Akumulator okna: (liczba zdarzeń, suma punktów). Liczony przyrostowo - w stanie okna jest jedna
 * para liczb, a nie lista zdarzeń. Wynik to ta sama para; klucz i granice okna dokłada
 * Diagnostics (wariant 3 z wykładu: aggregate + ProcessWindowFunction).
 */
public class CountSum
        implements AggregateFunction<HouseEvent, Tuple2<Long, Long>, Tuple2<Long, Long>> {

    @Override
    public Tuple2<Long, Long> createAccumulator() {
        return Tuple2.of(0L, 0L);
    }

    @Override
    public Tuple2<Long, Long> add(HouseEvent e, Tuple2<Long, Long> acc) {
        return Tuple2.of(acc.f0 + 1, acc.f1 + e.score);
    }

    @Override
    public Tuple2<Long, Long> getResult(Tuple2<Long, Long> acc) {
        return acc;
    }

    @Override
    public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
        return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
    }
}
