package org.example.flink.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import org.example.flink.model.Emission;

/**
 * Zawartość emisji: klucz, granice okna, stan akumulatora oraz watermark i czas przetwarzania
 * w chwili, gdy wyzwalacz zdecydował o emisji. Element wejściowy jest jeden - wynik CountSum.
 */
public class Diagnostics
        extends ProcessWindowFunction<Tuple2<Long, Long>, Emission, String, TimeWindow> {

    private final String branch;

    public Diagnostics(String branch) {
        this.branch = branch;
    }

    @Override
    public void process(String house, Context ctx, Iterable<Tuple2<Long, Long>> results,
                        Collector<Emission> out) {
        Tuple2<Long, Long> r = results.iterator().next();
        Emission e = new Emission();
        e.branch = branch;
        e.house = house;
        e.windowStart = ctx.window().getStart();
        e.windowEnd = ctx.window().getEnd();
        e.howMany = r.f0;
        e.sumScore = r.f1;
        e.wm = ctx.currentWatermark();
        e.emittedAt = ctx.currentProcessingTime();
        out.collect(e);
    }
}
