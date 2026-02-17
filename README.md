# Projekt Big Data: Kafka + Spark Structured Streaming

## 1. Cel projektu
Projekt jest pracą zaliczeniową, prezentującą kompletny, lokalny pipeline strumieniowy do przetwarzania zdarzeń zakupowych w czasie zbliżonym do rzeczywistego.

Cele praktyczne:
- wygenerować strumień syntetycznych zdarzeń (`producer.py`),
- przesłać zdarzenia przez Apache Kafka (topic `orders`),
- przetworzyć i zapisać dane przy użyciu Spark Structured Streaming (`kafka_spark_notebook.ipynb`),
- wykonać agregacje i wizualizacje wyników (`output/*.png`),
- monitorować jakość strumienia i opóźnienia konsumenta (`tracker.py`).

Cel merytoryczny:
- pokazać, jak architektura event-driven pozwala oddzielić produkcję danych od ich analizy,
- zmierzyć podstawowe metryki wydajności (throughput, opóźnienia, liczba błędów),
- przygotować powtarzalny scenariusz demonstracyjny dla obrony/projektu.

## 2. Zakres i problem badawczy
Projekt odpowiada na pytanie: czy lokalne środowisko oparte o 2 brokerów Kafka i Spark Structured Streaming pozwala stabilnie przetwarzać strumień zdarzeń, zachowując czytelną semantykę offsetów i możliwość raportowania metryk.

Badany zakres obejmuje:
- ingest i kolejkowanie danych (Kafka),
- przetwarzanie strumieniowe i utrwalenie danych (Spark),
- analizę wsadową na danych wynikowych (Spark/Pandas),
- wizualizacje końcowego wyniku (Seaborn/Matplotlib),
- kontrolę offsetów i pomiar opóźnień po stronie konsumenta.

## 3. Architektura rozwiązania

```
flowchart LR
    A[producer.py - generator syntetycznych zdarzeń JSON] -->|Publikacja zdarzeń| B[(Topik Kafka: orders)]
    B -->|Konsumpcja strumienia danych| C[Spark Structured Streaming - readStream]
    C -->|Transformacja JSON i zapis przyrostowy| D[(data/orders_json)]
    C -->|Utrwalenie stanu przetwarzania (checkpoint)| E[(data/_checkpoints/orders_json)]
    D --> F[Przetwarzanie wsadowe Spark: walidacja, czyszczenie i agregacje biznesowe]
    F --> G[Model danych do wizualizacji (Pandas)]
    G --> H[output/*.png]
    B --> I[tracker.py - konsument monitorujący metryki]
    I --> J[Rejestr metryk: przepustowość, opóźnienie, błędy, commit offsetów]
```

Opis logiczny:
1. Producent publikuje syntetyczne zdarzenia zakupowe w formacie JSON do topiku `orders`.
2. Apache Kafka realizuje buforowanie oraz replikację zdarzeń w konfiguracji dwubrokerowej.
3. Spark Structured Streaming konsumuje strumień z topiku, dokonuje parsowania JSON i utrwala rekordy w `data/orders_json`.
4. Stan przetwarzania strumieniowego jest zapisywany w `data/_checkpoints/orders_json`, co zapewnia możliwość wznowienia przetwarzania.
5. Na danych utrwalonych wykonywane jest przetwarzanie wsadowe obejmujące agregacje biznesowe (m.in. produkt, dzień tygodnia, godzina, przychód).
6. Wyniki agregacji są przekształcane do postaci analitycznej i wizualizowane jako zestaw wykresów zapisywanych do PNG.
7. Niezależny konsument monitorujący (`tracker.py`) raportuje metryki przetwarzania oraz realizuje ręczne zatwierdzanie offsetów.

## 4. Opis plików i kolejności skryptów

### `docker-compose.yaml`
Uruchamia lokalny klaster Kafka:
- 2 brokerów (`kafka-1`, `kafka-2`) w trybie KRaft (bez ZooKeepera),
- porty hosta: `9092` i `9094`,
- domyślna konfiguracja pod demo: 6 partycji, replication factor 2.

### `scripts/create_topic.sh`
Tworzy topik i od razu pokazuje jego konfigurację:
- domyślnie: `orders`, 6 partycji, RF=2,
- wykonuje polecenia `kafka-topics` wewnątrz kontenera `kafka-1`.

Przykład:
```bash
./scripts/create_topic.sh orders 6 2
```

### `producer.py`
Generator zdarzeń zamówień:
- losuje produkt, ilość, użytkownika i nadaje `event_time_ms`,
- publikuje rekordy do Kafka z kluczem `order_id`,
- sterowanie tempem: `--events-per-second`, `--duration-seconds`, `--max-events`,
- raportuje metryki: `sent_ok`, `sent_error`, `throughput_eps`, `avg_ack_ms`.

Przykład:
```bash
python producer.py --topic orders --events-per-second 100 --duration-seconds 120
```

### `producer_invalid.py`
Generator celowo niepoprawnych zdarzeń (test jakości danych):
- wysyła eventy z brakującą ceną (`unit_price`) i/lub ilością (`quantity`) albo z wartościami niepoprawnymi (`<= 0`),
- publikuje je do Kafka z kluczem `order_id`,
- pozwala wybrać tryb błędu przez `--invalid-mode` (domyślnie `random`).

Przykład:
```bash
python producer_invalid.py --topic orders --events-per-second 20 --duration-seconds 30 --invalid-mode random
```

### `tracker.py`
Konsument kontrolny/metryczny:
- subskrybuje topik i wypisuje odebrane zdarzenia,
- liczy opóźnienie end-to-end na podstawie `event_time_ms`,
- `enable.auto.commit=false`,
- ręczny commit offsetów co `N` wiadomości (`--commit-every`) i przy zamknięciu.

Przykład:
```bash
python tracker.py --topic orders --group-id order-tracker --commit-every 500
```

### `kafka_spark_notebook.ipynb`
Główna część analityczna:
1. Start `SparkSession` z pakietem `spark-sql-kafka-0-10`.
2. `readStream` z Kafka (`orders`, `startingOffsets=latest`).
3. Parsowanie JSON do rozszerzonego schematu zamówień (czas zakupu, dzień tygodnia, kanał, płatność, przychód).
4. Walidacja eventów i rozdzielenie strumienia na poprawne/niepoprawne.
5. `writeStream` poprawnych eventów do `data/orders_json` + checkpoint.
6. `writeStream` niepoprawnych eventów do `data/invalid_events` + osobny checkpoint.
7. Batch read z plików JSON, czyszczenie danych oraz lista eventów odrzuconych.
8. Agregacje pod wykres 3D i dodatkowe analizy biznesowe.
9. Konwersja do Pandas i wygenerowanie 4 wykresów.
10. Zapis wykresów do katalogu `output/`.

### `scripts/reset_project.py`
Skrypt "hard reset" do powtarzalnych uruchomień demo:
- (best effort) zatrzymuje lokalne procesy Jupyter/Spark oraz czyści pliki runtime Jupytera,
- wykonuje `docker compose down -v`, co usuwa kontenery i wolumeny brokerów Kafka (`kafka_1_data`, `kafka_2_data`),
- efekt `down -v`: czyszczone są dane brokerów, czyli m.in. topiki, offsety i logi (stan Kafki startuje od zera),
- wykonuje `docker compose up -d`, czeka na gotowość klastra i tworzy ponownie topik (domyślnie `orders`) przez `scripts/create_topic.sh`,
- usuwa i odtwarza katalogi `data/` oraz `output/` (czyści JSON-y, checkpointy i wygenerowane wykresy),
- czyści outputy i liczniki wykonania w `kafka_spark_notebook.ipynb`.

Ważne:
- jeśli użyjesz `--skip-kafka-restart`, topiki w Kafka nie są czyszczone (skrypt czyści wtedy tylko lokalne pliki projektu).

Przykład:
```bash
python scripts/reset_project.py
```

Szybkie czyszczenie topikow i zamkniecie kontenerow Kafka:
```bash
docker compose down -v
```

## 5. Scenariusz uruchomienia (zalecany porządek)
0. Instalacja Dockera (wymagana do uruchamiania `docker compose`):

- macOS (Docker Desktop, Homebrew):
```bash
brew install --cask docker
```
- Linux (Debian/Ubuntu):
```bash
sudo apt-get update
sudo apt-get install -y docker.io docker-compose-v2
sudo usermod -aG docker $USER
```
- Windows (Docker Desktop, PowerShell):
```powershell
winget install Docker.DockerDesktop
```

1. Instalacja `pyenv` (jeśli nie jest dostępny; `pyenv` to menedżer wersji Pythona):

- macOS (Homebrew):
```bash
brew install pyenv
```
- Linux (bash/zsh):
```bash
curl -fsSL https://pyenv.run | bash
```
- Windows (`pyenv-win`, PowerShell):
```powershell
winget install pyenv-win.pyenv-win
```

2. Przygotowanie środowiska - wariant A (zalecany, z `pyenv`):
```bash
pyenv install 3.11.14
pyenv local 3.11.14
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

3. Uruchomienie skryptu resetującego środowisko (zalecane przed każdym demem):
```bash
python scripts/reset_project.py
```

4. Start klastra Kafka (pomiń, jeśli wykonano krok 3):
```bash
docker compose up -d
```
5. Utworzenie topiku (pomiń, jeśli wykonano krok 3):
```bash
./scripts/create_topic.sh orders 6 2
```
6. **KROK OBOWIAZKOWY PRZED PRODUCENTAMI:** uruchom notebook [kafka_spark_notebook.ipynb](kafka_spark_notebook.ipynb) i wykonaj komorki streamingu (`Kafka -> JSON`).
   **Najpierw uruchom streaming w notebooku, dopiero potem uruchamiaj producentow (kroki 7 i 8).**
7. Start producenta:
```bash
python producer.py --topic orders --events-per-second 100 --duration-seconds 120
```
8. (Opcjonalnie) Start producenta błędnych eventów (test walidacji):
```bash
python producer_invalid.py --topic orders --events-per-second 20 --duration-seconds 30 --invalid-mode random
```
9. (Opcjonalnie) Start trackera w drugim terminalu:
```bash
python tracker.py --topic orders --group-id order-tracker --commit-every 500
```
10. Po zakończeniu producentów: uruchomić komórki batch/agregacja/wykres w notebooku.

## 6. Produkty analizy - wykresy biznesowe (prezentacja graficzna analizy eventów)

Uwaga: linki poniżej prowadzą do przykładowych wykresów z katalogu `example_charts` (to przykładowe wyniki, mogą różnić się od aktualnego uruchomienia).
- `output/orders_3d_item_weekday.png` - wykres 3D `produkt x dzień tygodnia x ilość` (kolor = przychód): pokazuje, jakie produkty i w które dni generują największy wolumen. Przykład: [orders_3d_item_weekday.png](example_charts/orders_3d_item_weekday.png)
- `output/orders_revenue_heatmap_weekday_hour.png` - heatmapa przychodu `dzień tygodnia x godzina`: wskazuje najlepsze okna czasowe na promocje i planowanie obsady. Przykład: [orders_revenue_heatmap_weekday_hour.png](example_charts/orders_revenue_heatmap_weekday_hour.png)
- `output/orders_pareto_revenue_item.png` - wykres Pareto przychodu po produkcie: identyfikuje produkty, które odpowiadają za największą część obrotu. Przykład: [orders_pareto_revenue_item.png](example_charts/orders_pareto_revenue_item.png)
- `output/orders_boxplot_channel_payment.png` - boxplot wartości zamówienia wg kanału sprzedaży i metody płatności: pomaga ocenić, gdzie i jak płacą klienci o wyższej wartości koszyka. Przykład: [orders_boxplot_channel_payment.png](example_charts/orders_boxplot_channel_payment.png)

## 7. Metryki techniczne (developerskie) i sposób ewaluacji

Rekomendowane metryki do raportu:
- producent: `throughput_eps`, `avg_ack_ms`, `sent_ok`, `sent_error`,
- konsument: `throughput_eps`, `avg_end_to_end_latency_ms`, `processed`, `errors`,
- analiza: liczba rekordów zapisanych przez Spark i wyniki agregacji biznesowych; produktami końcowymi są wykresy PNG zapisane w `output/`.

Interpretacja:
- wyższy `throughput_eps` oznacza większą przepustowość potoku,
- niższe `avg_ack_ms` i `avg_end_to_end_latency_ms` oznaczają mniejsze opóźnienia,
- rozbieżności między liczbą wysłanych i przetworzonych zdarzeń wymagają analizy offsetów i kolejności uruchomienia.

## 8. Użyte biblioteki i oprogramowanie

### Oprogramowanie systemowe
- Docker + Docker Compose (orkiestracja lokalnego klastra Kafka),
- Apache Kafka (Confluent image `cp-kafka:7.8.3`, KRaft),
- Apache Spark (PySpark `3.5.1`),
- pyenv (zalecany do ustawienia Python `3.11.14` zgodnie z `.python-version`),
- Jupyter Notebook.

### Biblioteki Python (`requirements.txt`)
- `confluent-kafka==2.8.2` - klient Kafka (producer/consumer),
- `faker==35.2.0` - generowanie danych syntetycznych,
- `pyspark==3.5.1` - przetwarzanie strumieniowe i batch,
- `pandas==2.2.3` - przygotowanie danych do wizualizacji,
- `seaborn==0.13.2` - wykresy statystyczne,
- `matplotlib==3.10.0` - backend wizualizacji,
- `jupyter==1.1.1` - uruchamianie notebooka.

## 9. Weryfikacja i diagnostyka techniczna (developerska)
Lista topików:
```bash
docker exec -it kafka-1 kafka-topics --list --bootstrap-server kafka-1:9092,kafka-2:9092
```

Opis topiku:
```bash
docker exec -it kafka-1 kafka-topics --describe --topic orders --bootstrap-server kafka-1:9092,kafka-2:9092
```

Podgląd wiadomości:
```bash
docker exec -it kafka-1 kafka-console-consumer --bootstrap-server kafka-1:9092,kafka-2:9092 --topic orders --from-beginning
```

## 9. Ograniczenia projektu
- Środowisko jest lokalne (single-host), więc wyników nie należy bezpośrednio przenosić na klaster produkcyjny.
- Projekt skupia się na demonstracji pipeline i metrykach runtime, a nie na pełnym hardeningu produkcyjnym.
- Notebook łączy część streamingową i batchową, (do celów dydaktycznych), ale w produkcji zwykle rozdziela się te role.
