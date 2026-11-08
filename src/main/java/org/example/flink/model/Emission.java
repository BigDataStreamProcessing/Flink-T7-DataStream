package org.example.flink.model;

/**
 * Jedna emisja wyniku okna - zapisywana jako JSON do tematu house-emissions.
 *
 * variant      - który wyzwalacz ją wysłał (default, count, continuous, purging, eotl, late-app)
 * house        - klucz
 * windowStart  - początek okna (epoch ms)
 * windowEnd    - koniec okna (epoch ms, bez końca)
 * howMany      - liczba zdarzeń w stanie okna w chwili emisji
 * sumScore     - suma punktów w stanie okna w chwili emisji
 * wm           - watermark operatora w chwili emisji (epoch ms)
 * emittedAt    - czas przetwarzania w chwili emisji (epoch ms)
 */
public class Emission {
    public String variant;
    public String house;
    public long windowStart;
    public long windowEnd;
    public long howMany;
    public long sumScore;
    public long wm;
    public long emittedAt;

    public Emission() {}

    @Override
    public String toString() {
        return "Emission{" + variant + ", " + house + ", [" + windowStart + ", " + windowEnd + "), n="
                + howMany + ", sum=" + sumScore + ", wm=" + wm + ", at=" + emittedAt + "}";
    }
}
