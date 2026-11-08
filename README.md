# Flink-T7-DataStream

Projekt startowy do warsztatu **T7 - od Table API do DataStream API** ze szkolenia
"Zaawansowane przetwarzanie strumieni danych w Apache Flink" (scenariusz *HouseStats*,
środowisko *BigData26*, Apache Flink 2.3.0, *DataStream API* v1).

Zawartość (`src/main/java/org/example/flink`):

- `TriggersApp` - część A: jedno okno stałe 1 min w czasie zdarzenia i pięć gałęzi z różnymi
  wyzwalaczami (domyślny, `CountTrigger`, `ContinuousEventTimeTrigger`, `PurgingTrigger`,
  własny `EarlyOnTimeLate`); każda emisja trafia do tematu `house-emissions`,
- `LateApp` - część B: źródło w *SQL* z małym zapasem watermarku, `toDataStream`, okno z
  `allowedLateness` i wyjściem bocznym (temat `house-late`), kolejne wersje wyniku z powrotem do
  *SQL* (`fromChangelogStream`, upsert) i do tabeli `housestats` w MySQL,
- `ChangelogApp` - "Jeśli masz czas": `toChangelogStream` dla agregacji bez okna,
- gotowe klasy pomocnicze: `HouseEvent` (POJO zdarzenia), `Emission` (jedna emisja okna),
  `CountSum` (akumulator: liczba i suma), `Diagnostics` (`ProcessWindowFunction` dokładająca
  klucz, granice okna, watermark i czas emisji), `EarlyOnTimeLate` (szkielet wyzwalacza),
  `KafkaSinks` (ujście Kafka z JSON).

Miejsca do uzupełnienia oznaczone są komentarzami `TODO`.

## Wymagania

- Java 17 lub 21, Maven (IntelliJ IDEA ma wbudowany),
- działające środowisko BigData26 (profil `flink`; tematy `kafka-input`, `house-emissions`,
  `house-late`; MySQL z bazą `streamdb` i tabelą `housestats` - przygotowanie w warsztacie),
- projekt sklonowany do `~/BigData26/shared_workspace` - katalog jest widoczny w kontenerze
  `flink-jobmanager` jako `/opt/workspace`.

## Budowa i uruchomienie

1. Otwórz katalog projektu w IntelliJ IDEA (*File -> Open*), zaufaj projektowi Maven.
2. Zbuduj jar: panel *Maven* -> *Lifecycle* -> `clean`, potem `package`
   (powstaje `target/flink-t7-datastream.jar`).
3. Wyślij wybrany program na klaster (jar nie wskazuje klasy głównej - podajemy ją opcją `-c`):

   ```bash
   cd ~/BigData26
   docker compose exec flink-jobmanager ./bin/flink run -d \
     -c org.example.flink.TriggersApp /opt/workspace/Flink-T7-DataStream/target/flink-t7-datastream.jar
   ```

Wszystkie zależności (w tym złącza Kafki i JDBC, sterownik MySQL i format JSON) mają zakres
`provided` - klaster ma je w `/opt/flink/lib`. Programy korzystają z adresów z sieci kontenerów
(`kafka-1..3`, `mysql`), więc są przeznaczone do uruchamiania na klastrze.

Treść zadań: warsztat T7 (MkDocs, `apache-flink/w7`).
