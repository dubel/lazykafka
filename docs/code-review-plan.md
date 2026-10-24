# Przegląd kodu lazykafka: luki i plan usprawnień

Stan na: 2026-10-24, wersja `v0.0.18` (commit `60de465`).

Zakres: `main.go`, `kafka/`, `config/`, `ui/`, `demo/`, `Dockerfile`, `.goreleaser.yaml`, `.github/workflows/`.
Weryfikacja: `go vet ./...` czysto, `go test -race ./...` zielone (ale `ui/` nie ma testów jednostkowych).
Dodatkowo uruchomiłem aplikację na lokalnym brokerze Kafka 4.0.0 (KRaft) z danymi demo z `demo/seed-demo.sh`.

**Status wdrożenia (2026-10-24):** naprawione #1–#7 oraz #9–#12 (#11 tylko próg 3 kolejnych błędów, bez auto-reconnectu).
Otwarte: #8 (locki przez operacje sieciowe), auto-reconnect z #11 oraz cały P2 i P3.

Priorytety: **P0** to błąd widoczny dla użytkownika lub ryzyko utraty danych, **P1** to poprawność i współbieżność,
**P2** to luki funkcjonalne, **P3** to porządki i utrzymanie.

---

## P0: błędy widoczne dla użytkownika

### 1. Cluster State ładuje się 20 s lub dłużej, gdy aktywna jest konsumpcja

- **Gdzie:** `kafka/manager.go` `GetClusterState`.
- **Pomiar:** na tym samym klastrze (28 topiców, ~80 partycji) bez konsumpcji: **73 ms**; z aktywnym `ConsumeTopic`: **20,6 s**.
  W UI użytkownik widzi wtedy "Loading cluster state..." przez ponad 20 s.
- **Przyczyna:** dla każdej partycji wykonywane są sekwencyjnie dwa `Client.GetOffset` (newest i oldest) na **współdzielonym**
  kliencie sarama. Konsument używa tego samego połączenia do brokera i wysyła fetch z long-pollem (`MaxWaitTime`).
  Kafka przetwarza żądania z jednego połączenia po kolei, więc każde ListOffsets czeka za fetchem (head-of-line blocking).
  Przy ~160 żądaniach daje to ~20 s.
- **Naprawa:**
  1. Pobierać offsety zbiorczo: jedno `OffsetRequest` na brokera-lidera ze wszystkimi partycjami (`AddBlock`),
     osobno dla `OffsetNewest` i `OffsetOldest`. Zamiast ~160 żądań będą 2 na brokera.
  2. Konsumenta tworzyć na **osobnym** `sarama.Client`, żeby fetch long-poll nie blokował metadanych, health-checka ani admina.
  3. Zastąpić goroutine-per-topic (bez limitu) zbiorczymi żądaniami. Przy tysiącach topiców obecny kod uruchamia tysiące goroutine.

### 2. Spóźniony wynik Cluster State nadpisuje ekran po Esc

- **Gdzie:** `ui/cluster_state.go` `showClusterStateOnPage`.
- Esc na ekranie "Loading" wraca do siatki, ale goroutine dalej czeka na `GetClusterState` i potem wywołuje
  `renderClusterState`, który robi `SetRoot`. Widok wyskakuje nagle, nawet gdy użytkownik jest już w innym modalu.
- **Naprawa:** `context.WithCancel` + `GetClusterState(ctx)` z timeoutem (np. 10 s). Esc anuluje kontekst. Przed `SetRoot`
  sprawdzić, czy kontekst nie został anulowany (lub porównać token żądania).

### 3. Usunięcie topicu bez potwierdzenia

- **Gdzie:** `ui/topics.go` `deleteSelectedTopic` (klawisz `r`).
- Jedno przypadkowe `r` na produkcyjnym klastrze kasuje topic z danymi. Usuwanie consumer group ma już modal potwierdzenia
  (`cluster_state.go`), topic nie ma.
- **Naprawa:** ten sam wzorzec modalu co dla consumer group. Najlepiej z wpisaniem nazwy topicu przy usuwaniu.
  Usunięcie brokera z listy (`brokers.go` `removeCurrentBroker`) też bez potwierdzenia, ale to tylko wpis w konfiguracji.

### 4. Treść wiadomości interpretowana jako tagi kolorów tview

- **Gdzie:** `ui/consume.go` (`fmt.Fprintln(p.TextView, msg)` przy `SetDynamicColors(true)`), także nazwy topiców i brokerów
  w listach (`"[white:green]" + name`).
- Wiadomość zawierająca np. `[red]` albo `["x"]` zostanie pokolorowana lub obcięta. Użytkownik widzi inną treść niż w Kafce.
- **Naprawa:** `tview.Escape(msg)` dla danych z zewnątrz (payload, nazwy topiców, adresy, aliasy).

### 5. Konsumpcja może zablokować goroutine na zawsze (wyciek przy przełączaniu topiców)

- **Gdzie:** `kafka/manager.go` `ConsumeTopic`.
- Goroutine partycji robi `messages <- ...` i `errors <- err` bez `select` na `ctx.Done()`. Kanały są niebuforowane, a czytelnik
  w `ui/consume.go` kończy pętlę po `ctx.Done()`. Jeśli wiadomość przyjdzie w momencie anulowania, wysyłka blokuje się na zawsze.
  `wg.Wait()` nigdy się nie kończy, a `PartitionConsumer` i `Consumer` nie zostają zamknięte. Przy szybkim przełączaniu topiców
  wycieka goroutine i zasoby brokera.
- **Naprawa:** każda wysyłka jako `select { case messages <- m: case <-ctx.Done(): return }`. To samo dla `errors`,
  także dla wczesnych `errors <- err` przed pętlą.

---

## P1: współbieżność i poprawność

### 6. Produkcja blokuje wątek UI

- **Gdzie:** `ui/produce.go` handler Alt+Enter / Ctrl+S.
- `ProduceMessage` (synchroniczny `SyncProducer`, timeouty 10 s) wołany jest bezpośrednio w input handlerze tview,
  do tego pod `RLock`. Przy wolnym lub niedostępnym brokerze cały interfejs zamarza. Narusza to regułę z `AGENTS.md`.
  Publikacja pliku (`filepicker.go`) robi to już poprawnie w goroutine.
- **Naprawa:** jak w `publishFileContent`: goroutine + `QueueUpdateDraw`. Na czas wysyłki stan "Sending...".

### 7. Data race'y w panelu Consume i przy łączeniu

- `ConsumePanel.LastMessage` zapisywane w goroutine, czytane w UI (`copyLastMessageToClipboard`).
- `ConsumePanel.cancelFunc` i `CurrentTopic`: `StopConsuming()` wołane z goroutine `ConnectToBroker` i `handleConnectionLost`,
  a `StartConsuming` z wątku UI.
- `App.healthCheckCancel`: zapis z goroutine `ConnectToBroker`, bez synchronizacji.
- `TextView.SetChangedFunc` wywołuje `ScrollToEnd()` i `Draw()`. tview odpala changed-func **w osobnej goroutine**,
  więc `ScrollToEnd` modyfikuje widget poza wątkiem UI. Do tego każda wiadomość powoduje dodatkowy `Draw()`, czyli podwójne
  przerysowanie.
- **Naprawa:** stan panelu Consume zmieniać tylko w wątku UI (przez `QueueUpdate`), `ScrollToEnd` przenieść do tego samego
  `QueueUpdateDraw`, który dopisuje wiadomość, i usunąć changed-func. `cancelFunc` i `healthCheckCancel` chronić mutexem.
  Dodać test z `-race` symulujący szybkie przełączanie topiców.

### 8. Locki trzymane przez operacje sieciowe

- `LoadTopics` trzyma `RLock` w czasie `RefreshMetadata`, `CreateTopic`/`DeleteTopic` przez całe wywołanie admina.
  `ConnectToBroker` i `handleConnectionLost` potrzebują `Lock`, więc czekają na operację sieciową (do ~10 s), a nowe `RLock`
  ustawiają się za writerem. W efekcie UI chwilowo stoi.
- **Naprawa:** jeden spójny wzorzec, np. snapshot wskaźnika pod `RLock` (jak w health-checku), a `Manager.Close()`
  odporny na równoległe wywołania (operacja dostaje błąd "client closed" zamiast panic). Alternatywnie `Manager` z własnym
  licznikiem referencji.

### 9. Nieograniczony bufor konsumpcji i floodowanie UI

- `TextView` rośnie bez limitu. Tryb "from beginning" na dużym topicu daje setki tysięcy linii i jeden `QueueUpdateDraw`
  na wiadomość, przez co UI się dławi, a pamięć rośnie.
- **Naprawa:** `TextView.SetMaxLines(N)` (np. 2000, konfigurowalne) i batchowanie: zbierać wiadomości i flushować co ~50 ms
  jednym `QueueUpdateDraw`. Licznik "N msgs" w tytule.

### 10. Zapis konfiguracji: ignorowane błędy, zapis nieatomowy

- `config.SaveConfig` zwraca błąd, a `brokers.go` go ignoruje (3 miejsca). Użytkownik nie wie, że zmiana się nie zapisała.
- `os.Create` + encode: przerwanie w trakcie zostawia pusty lub ucięty `brokers.json`. Przy następnym starcie `LoadConfig`
  zwraca błąd, `NewApp` podstawia pustą listę, a pierwszy zapis **kasuje wszystkie brokery**.
  Analogicznie `LoadSettings` przy błędzie parsowania zwraca domyślne i następny zapis nadpisuje plik.
- **Naprawa:** zapis do pliku tymczasowego + `os.Rename`, uprawnienia `0600` (tym bardziej po dodaniu credentiali, patrz #12),
  przy błędzie parsowania backup `brokers.json.bak` i komunikat w UI zamiast cichego resetu.

### 11. Health-check rozłącza po jednym nieudanym pingu

- `startHealthCheck`: pierwszy błąd `RefreshMetadata` wywołuje `handleConnectionLost`, który zamyka managera, czyści UI
  i przerywa konsumpcję. Krótka czkawka sieci albo opóźnienie przez head-of-line blocking (#1) zrywa sesję.
- **Naprawa:** próg 2–3 kolejnych błędów, potem automatyczny reconnect z backoffem i wznowienie konsumpcji bieżącego topicu.
  Status połączenia w stopce zamiast twardego modala.

### 12. Payloady wiadomości w logach

- `produce.go` loguje pełną treść (`Sending message to topic '%s': %s`). Logi leżą w `~/.lazykafka` i są pokazywane pod F5.
  Na produkcji to wyciek danych (PII, tokeny).
- **Naprawa:** logować topic + rozmiar, treść tylko w trybie debug (flaga / env).

---

## P2: luki funkcjonalne (największa wartość dla użytkowników)

### 13. Brak TLS i SASL (blokuje użycie z chmurowymi klastrami)

- `kafka/client.go` nie ustawia `Net.TLS` ani `Net.SASL`. MSK, Confluent Cloud, Aiven, Redpanda Cloud i większość firmowych
  klastrów są poza zasięgiem. `Dockerfile` instaluje `ca-certificates` "na wypadek TLS", ale kod tego nie używa.
- **Plan:**
  - `config.Broker`: `security_protocol` (PLAINTEXT/SSL/SASL_PLAINTEXT/SASL_SSL), `sasl_mechanism` (PLAIN, SCRAM-SHA-256/512),
    `username`, `ca_file`, `cert_file`/`key_file`, `insecure_skip_verify`.
  - Hasło nie trafia do `brokers.json` jawnym tekstem: odwołanie do zmiennej środowiskowej (`password_env`) lub keyring systemowy.
  - Jedna funkcja budująca `sarama.Config` dla klienta, producenta i admina. Dziś są dwie kopie: `NewClient` i `getAdminConfig`.
  - Formularz brokera: zakładka "Security".

### 14. Wiele adresów bootstrap

- `ConnectToBroker` przekazuje jeden adres. Gdy ten broker leży, połączenie się nie uda, mimo że klaster działa.
- **Plan:** pole adresu przyjmuje listę po przecinku, `NewManager(strings.Split(...))`. Dialer fallback (`bootstrapFallbackDialer`)
  bierze dziś `brokers[0]`. Do przemyślenia, czy fallback ma iterować po wszystkich.

### 15. Podgląd wiadomości jest ubogi

- Wyświetlane jest tylko `[P<n>] value`. Brakuje klucza, offsetu, timestampu i nagłówków, a to codzienne potrzeby przy debugowaniu.
- **Plan:**
  - `ConsumeTopic` zwraca strukturę (`Partition, Offset, Key, Value, Timestamp, Headers`) zamiast sformatowanego stringa.
    Formatowanie należy do UI.
  - Linia: `P1 @1234 12:03:44 key=ORD-1  {...}`, przełączniki pokazywania pól.
  - Enter na wiadomości: widok szczegółów z pretty-print JSON (i kopiowaniem całości).
  - Wartości binarne: hex/base64 zamiast surowych bajtów do terminala.
  - Wyszukiwanie / filtr w skonsumowanych wiadomościach (`/` jak w topicach).

### 16. Start konsumpcji tylko "newest" albo "oldest"

- **Plan:** start od offsetu, od timestampu (`GetOffset` z czasem w ms) i "ostatnie N na partycję". Wybór partycji.
  Wszystko bez consumer group, zgodnie z filozofią "no footprints" z README.

### 17. Produkcja bez klucza i nagłówków

- `ProduceMessage(topic, value)`: brak klucza oznacza brak kontroli partycjonowania, czyli nie da się odtworzyć realnego zdarzenia.
- **Plan:** opcjonalne pola key / headers / partycja w panelu Produce (np. pierwsze linie w formacie `key: ...`, albo mini-formularz).
  Akcja "re-produce" z widoku szczegółów wiadomości.

### 18. Tworzenie topicu zawsze z 1 partycją i RF=1

- `CreateTopic` hardkoduje `NumPartitions: 1, ReplicationFactor: 1`. Na produkcyjnym klastrze z `min.insync.replicas=2`
  taki topic nie przyjmie zapisów z `acks=all`.
- **Plan:** w formularzu partycje i RF z domyślnym `-1` (broker defaults, CreateTopics v4+, sarama wspiera) plus opcjonalne configi
  (`retention.ms`, `cleanup.policy`).

### 19. Szczegóły topicu

- Cluster State pokazuje offsety zbiorczo. Brakuje widoku pojedynczego topicu: partycje, lider, repliki/ISR, offsety per partycja,
  config topicu, które consumer groupy go czytają i z jakim lagiem. Dane są już w dużej mierze pobierane w `GetClusterState`.

### 20. Drobne UX

- Esc w głównym widoku od razu zamyka aplikację. Łatwo wyjść przypadkiem (Esc służy też do zamykania modali i filtra).
  Propozycja: `q` / Ctrl+C do wyjścia, Esc tylko "wstecz", albo potwierdzenie.
- Brak ekranu pomocy (`?`) z listą skrótów. Dziś skróty są tylko w stopkach i README.
- Długie nazwy topiców ucinają tytuł panelu Consume od lewej (przy 120 kolumnach widać `nsume (F3) - contemporary-...`).
  Skrócić status (`[new]`/`[from 0]`) i ucinać nazwę topicu w środku.
- Ustawienie `check_for_updates` istnieje w `Settings`, ale nic go nie używa. Zaimplementować albo usunąć.
- Motywy: commit "Battling with themes" został zrewertowany. Najprostszy krok to używanie kolorów terminala (`tcell.ColorDefault`
  zamiast twardego `ColorWhite`/`ColorGreen`), bez pełnego systemu motywów.

---

## P3: architektura, testy, repo, release

### 21. Duplikacja w UI

- Logika podświetlenia "`>> selected <<`" / "`>>connected<<`" (focus/blur/changed) skopiowana w `topics.go` i `brokers.go`.
- `SetInputCapture` nawigacji formularza (Tab/strzałki) skopiowany 3 razy, zamykanie modala (`InModal=false; SetRoot(Grid)`)
  w kilkunastu miejscach, `showTemporaryMessage` w 3 panelach z osobnymi stałymi tekstów stopek.
- **Plan:** helpery `App.openModal(p)`/`App.closeModal(focus)`, `newNavForm(title)`, typ `statusFooter` (domyślny tekst + temporary
  message + timer) i `highlightList` (lista z jednym oznaczonym elementem). Mniej kodu i jedno miejsce na poprawki z #4.

### 22. Admin client tworzony przy każdej operacji

- `CreateTopic`, `DeleteTopic`, `DeleteConsumerGroup`, `GetClusterState` za każdym razem robią `sarama.NewClusterAdmin`
  (nowe połączenia, handshake), a kod logowania / zamykania jest skopiowany 3 razy.
- **Plan:** jeden `ClusterAdmin` na osobnym kliencie, tworzony leniwie w `Manager` i zamykany w `Close()`. Razem z #1 i #13
  wychodzą 3 klienty: metadata/producer, consumer, admin, z jedną funkcją konfiguracji.

### 23. Testy i CI

- `ui/` nie ma testów jednostkowych, jest tylko `tui_simulation_test.go` za tagiem `integration`. Workflow GitHub to tylko release:
  testy nie są uruchamiane przy push/PR.
- **Plan:**
  - Workflow `ci.yml`: `go vet`, `go test -race ./...`, `golangci-lint`, integracja z Kafką jako service container.
  - Testy jednostkowe UI na `tcell.SimulationScreen` (bez brokera, z fałszywym managerem). Wymaga wydzielenia interfejsu
    po stronie UI (`TopicLister`, `Producer`, ...), w miejscu użycia, nie w `kafka/`.
  - Test regresyjny dla #1 (czas `GetClusterState` przy aktywnym konsumencie) i #5 (brak wycieku goroutine: `goleak`).
  - Macierz wersji brokera: obecny compose to Kafka 3.7.0. Sprawdziłem ręcznie, że podstawowe funkcje działają też na 4.0.0,
    ale warto mieć to w CI.

### 24. Higiena repo

- Zbudowana binarka `lazykafka` (13 MB) jest **w gicie** (`git status` pokazuje `M lazykafka`). W gicie są też `.idea/`
  i `.lazykafka/app.log`. `.gitignore` ma tylko `*.log` i `.stuff/`.
- **Plan:** `git rm --cached lazykafka .idea -r .lazykafka -r`, dopisać do `.gitignore`. Rozważyć wyczyszczenie historii, bo binarki
  puchną z każdą wersją.
- `go.mod`: wszystkie zależności oznaczone `// indirect`, także bezpośrednie (`sarama`, `tview`, `tcell`). `go mod tidy` + commit.
  Hook goreleasera `go mod tidy` w CI zmienia wtedy drzewo w trakcie release.

### 25. Release i Docker

- `.goreleaser.yaml` ustawia `-X main.commit` i `-X main.date`, ale w `main.go` istnieje tylko `version`. Flagi są po cichu
  ignorowane. Dodać zmienne (i pokazać je na splashu / `--version`) albo usunąć.
- Brak flagi `--version` w CLI (przydatne dla brew/issue reportów).
- `Dockerfile`: `alpine:latest` bez przypiętej wersji, proces jako root, przestarzałe `-installsuffix cgo`, brak `-trimpath`
  i wersji w ldflags (obraz bierze wersję z osadzonego pliku, to akurat działa).
  **Plan:** przypiąć wersję alpine (lub `distroless/static`), `USER` nie-root, `-trimpath -ldflags "-s -w -X main.version=..."`.
  Ewentualnie budować obraz w goreleaserze (`dockers:`) zamiast osobnego `make publish-ghcr`.

### 26. Dokumentacja i skrypty demo

- `README.md` w src rozjeżdża się z `lazykafka-dist/README.md` i z kodem: wspomina `lazykafka.log` (dziś dzienne pliki
  `lazykafka-DD-MM-YYYY.log` z rotacją do `old/`), nie opisuje Cluster State (`d`), file pickera (Ctrl+P) ani kopiowania (`c`).
- `.stuff/record_features_*.sh` zapisuje do `settings.json` klucz `LastFilePickerDir`, a aplikacja czyta `last_file_picker_dir`.
  Ustawienie katalogu file pickera w nagraniach nie działało.
- `demo/seed-demo.sh` i `live-producer.sh` mają na sztywno `KAFKA_BIN=/opt/kafka/bin`. Zmienna środowiskowa jak dla `KAFKA_BROKER`
  pozwoliłaby seedować dane bez Dockera (tak nagrałem film promocyjny, z lokalną Kafką na Javie).

---

## Proponowana kolejność

1. **Sprint stabilizacyjny (P0 + #6, #7):** zbiorcze offsety + osobny klient konsumenta (#1), anulowanie Cluster State (#2),
   potwierdzenie usunięcia topicu (#3), `tview.Escape` (#4), `select` na `ctx` w `ConsumeTopic` (#5), asynchroniczny produce (#6),
   porządek w wątkach panelu Consume (#7). Do tego CI z `-race` (#23), żeby to nie wróciło.
2. **Odporność:** locki (#8), limit i batchowanie konsumpcji (#9), atomowy zapis konfiguracji (#10), health-check z reconnectem (#11),
   logi bez payloadów (#12).
3. **Funkcje dla realnych klastrów:** TLS/SASL (#13), wiele bootstrapów (#14). To odblokowuje użytkowników chmurowych.
4. **Funkcje debugowania:** bogatszy podgląd wiadomości (#15), start od offsetu / czasu (#16), key / headers w produce (#17),
   parametry topicu (#18), widok topicu (#19), UX (#20).
5. **Porządki:** refaktor UI (#21), admin client (#22), repo / release / Docker / docs (#24–#26). Część warto zrobić przy okazji
   punktów 1–3, np. #21 razem z #3 i #4.
