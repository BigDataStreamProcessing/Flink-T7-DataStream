package org.example.flink;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

/**
 * Własny wyzwalacz okna czasu zdarzenia, odpowiednik złożonego wyzwalacza z wykładu:
 * - E (early)   - wczesne wyniki co "every" CZASU PRZETWARZANIA, dopóki okno jest otwarte,
 * - O (on time) - wynik, gdy watermark minie koniec okna,
 * - L (late)    - poprawka natychmiast po każdym spóźnionym zdarzeniu (działa tylko, gdy okno
 *                 czeka na spóźnione - allowedLateness > 0).
 * Emisje są narastające (FIRE): każda zawiera cały dotychczasowy stan okna.
 */
public class EarlyOnTimeLate extends Trigger<Object, TimeWindow> {

    private final long every;

    private EarlyOnTimeLate(long every) {
        this.every = every;
    }

    public static EarlyOnTimeLate of(Duration every) {
        return new EarlyOnTimeLate(every.toMillis());
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window,
                                   TriggerContext ctx) throws Exception {
        // TODO 4a:
        //  - L: jeśli watermark już minął koniec okna (window.maxTimestamp()), emituj od razu,
        //  - O: w przeciwnym razie zarejestruj timer czasu zdarzenia na koniec okna,
        //  - E: oraz timer czasu przetwarzania na najbliższą pełną wielokrotność "every"
        //       (now - now % every + every, gdzie now = ctx.getCurrentProcessingTime()).
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx)
            throws Exception {
        // TODO 4b: E - emituj, ale tylko dopóki okno nie zostało zamknięte przez watermark
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx)
            throws Exception {
        // TODO 4c: O - emituj, gdy strzela timer końca okna
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    @Override
    public String toString() {
        return "EarlyOnTimeLate(" + every + " ms)";
    }
}
