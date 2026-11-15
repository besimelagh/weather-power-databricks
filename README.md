# Værbasert strømprisanalyse

En liten, men produksjonsformet Databricks-løsning for å analysere sammenhengen mellom timepris på strøm i NO1 og vær i Oslo. Den er bevisst dimensjonert for Databricks Free Edition: én sone, en kort historisk periode og ingen unødvendig streaming.

## Hva løsningen demonstrerer

```text
Open-Meteo historical API ─┐
                           ├── Bronze: immutable API JSON + teknisk metadata
Hvakosterstrømmen API ────┘
                                      │
                                      ▼
                    Silver: validerte time-data, UTC-normalisering,
                    deduplisering og dokumentert manglende-data-policy
                                      │
                                      ▼
                    Gold: pris per temperaturintervall og daglig trend
                                      │
                                      ▼
                              Notebook-visualisering
```

| Lag | Objekter | Ansvar |
| --- | --- | --- |
| Bronze | `weather_power_bronze.api_payloads` | Originale JSON-payloads med hash, URL, HTTP-status, tidspunkt og run-id. |
| Silver | `weather_hourly`, `price_hourly`, `hourly_weather_price` | Parse, plausibilitetskontroll, UTC-tidsnøkkel og timesjoin. |
| Gold | `price_by_temperature_band`, `daily_weather_price` | Forbruksklare analyser og visualisering. |
| Ops | `ingestion_errors`, `quarantine_hourly`, `data_quality_results` | Feil, isolerte rader og målbare DQ-resultater. |

Standardkatalogen er `workspace`, slik at fulle navn blir for eksempel `workspace.weather_power_silver.hourly_weather_price`. I en vanlig organisasjon bruker jeg én katalog per miljø og schemas `bronze`, `silver`, `gold` og `ops`.

## Datakildevalg og avgrensninger

- **Pris:** [Hvakosterstrømmen API](https://www.hvakosterstrommen.no/strompris-api), `NO1`. Responsen gir timepris i NOK/kWh og ISO-8601-tid med offset. Prisene er spotpris ekskl. nettleie, mva. og strømstøtte; dette er bevisst ikke presentert som en sluttbrukerpris.
- **Vær:** [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api), Oslo (`59.9139, 10.7522`), med temperatur, vindhastighet og nedbør. Dette er historisk reanalyse/modellerte værdata, ikke stasjonsmålinger. Den nøkkelfrie tilgangen gjør en Free Edition-demo reproduserbar.

For en norsk produksjonsløsning ville jeg normalt vurdert **MET Frost** som primærkilde for målte stasjonsdata, med `client_id` i Databricks Secrets. Jeg har valgt Open-Meteo her fordi oppgaven eksplisitt tillater andre åpne kilder og fordi demoen da ikke avhenger av at en intervjuer har tilgang til en personlig Frost-nøkkel. Oslo er kun en geografisk proxy for NO1; en produksjonsanalyse bør bruke flere stasjoner eller griddede, vektede værdata.

## Kjøring i Databricks Free Edition

### 1. Importer eller sync prosjektet

Den enkleste demonstrasjonen er å importere filene under `src/notebooks/` til en Databricks-mappe. Kjør dem i numerisk rekkefølge. Alternativt kan prosjektet deployes som en Declarative Automation Bundle:

```powershell
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run weather_power_price -t dev --params run_date=2025-11-10,ingestion_mode=api
```

Kommandoene over forutsetter en konfigurert Databricks CLI-profil. Jobben er opprettet med planlagt kjøring **pauset**; aktiver den først når du har validert dataene.

Før første kjøring: verifiser at du har rettigheter til å opprette schemas i den valgte katalogen. I en personlig Free Edition-workspace er dette normalt `workspace`, men ikke anta det i en delt workspace:

```sql
SELECT current_catalog(), current_schema();
SHOW GRANTS ON CATALOG workspace;
```

`00_setup.py` feiler med en eksplisitt melding dersom `CREATE SCHEMA` mangler. Det er riktig å løse tilgang som en plattformforutsetning, ikke å omgå Unity Catalog ved å skrive til tilfeldige filstier.

### 2. Velg en stabil demodato

Sett widgeten `run_date` til en dato som ligger minst omtrent en uke tilbake, for eksempel `2025-11-10`. Det unngår forsinkelse i historiske værdata; samme konservative T-7 er kode-standarden dersom datoen ikke angis. Sett `catalog` til `workspace` hvis du bruker Free Edition.

### 3. API-modus eller snapshot-modus

Start med `ingestion_mode=api`. Databricks Free Edition kan begrense utgående internett for uverifiserte kontoer. Hvis API-kallene blokkeres, er det en kjent plattformbegrensning – ikke en feil som skal skjules i demoen.

`02_ingest_prices.py` prøver først pris-API-et med avgrenset retry. Dersom kilden fortsatt er utilgjengelig, bruker den automatisk den opplastede pris-snapshoten og registrerer `ingestion_mode=snapshot_fallback` i Bronze. Denne fallbacken gjelder bare tilgjengelighetsfeil; 4xx, ugyldig JSON og andre kontraktsbrudd feiler fortsatt raskt og synlig. Værinnhentingen bruker fortsatt API direkte, eller snapshot når `ingestion_mode=snapshot` er valgt.

Bruk da den dokumenterte Free Edition-tilpasningen:

```powershell
python scripts/acquire_raw_snapshots.py --date 2025-11-10 --out sample_data/raw_snapshots
```

Last opp de to små JSON-filene til UC-volumet som opprettes av `00_setup.py`:

```text
/Volumes/workspace/weather_power_bronze/raw_snapshots/open_meteo/NO1/2025-11-10.json
/Volumes/workspace/weather_power_bronze/raw_snapshots/hvakosterstrommen/NO1/2025-11-10.json
```

Kjør deretter pipeline på nytt. Velg `ingestion_mode=snapshot` dersom begge kildene må lese snapshots; ellers kan pris-snapshoten brukes som automatisk fallback mens værdata hentes fra API-et. Resten av flyten – Bronze-metadata, Delta-MERGE, Silver-transformasjon, DQ og Gold – er identisk. I produksjon ligger API-klienten inne i den godkjente datanettsonen; snapshot-sporet er kun en ærlig Free Edition-workaround.

### 4. Kjør og vis resultatet

1. `00_setup.py`
2. `01_ingest_weather.py` og `02_ingest_prices.py` (kan kjøres parallelt)
3. `03_silver_transform.py`
4. `05_quality.py` – stopper på kritiske feil og skriver alltid DQ-resultater først
5. `04_gold.py`
6. `06_visualize.py` – viser søylediagrammet *gjennomsnittlig NOK/kWh per temperaturintervall*

For en mer meningsfull visualisering før intervjuet, kjør valgfri `07_backfill_demo.py` med `end_date=2025-11-10` og `lookback_days=30`. Den kjører samme idempotente daglige flyt for et lite historisk vindu og bygger Gold til slutt. Dette er fortsatt svært lite for Free Edition, men gir nok temperaturvariasjon til et godt diagram. Ikke legg backfill-driveren på daglig schedule.

## Viktige tekniske valg

### Tid og sommertid

Spark-sessionen settes til UTC. Strømprisens offset-bevarte tidsstempel og værens UTC-tid normaliseres til `hour_start_utc`, som er eneste join-nøkkel. Værkallet henter én ekstra UTC-dag på hver side og avleder deretter lokal `Europe/Oslo`-dato; dermed dekker det første og siste prisintervallet også ved sommertid. Data-kvalitetsregelen godtar 23–25 timer per lokal dato, ikke hardkodede 24.

### Manglende verdier

| Situasjon | Policy |
| --- | --- |
| Manglende/ugyldig pris | Aldri imputer. Raden havner i quarantine eller blir synlig som avvik; kritiske kontraktbrudd stopper Gold. |
| Én intern manglende værtime | Temperatur og vind interpoleres lineært bare når både forrige og neste komplette time finnes. Raden flagges `interpolated_temp_wind`. |
| Lengre værhull eller manglende nedbør | Behold `NULL`; nedbør blir aldri erstattet med null. `weather_handling` og dekning bevares i fakta-tabellen. |
| Korrigert kildepayload | Ny hash beholdes i Bronze; Silver `MERGE` oppdaterer den naturlige time-nøkkelen deterministisk. |

### Idempotens og feilbehandling

- Bronze `MERGE` bruker `(source, zone, business_date, payload_sha256)`. Identisk rerun legger ikke inn ny rådata.
- Silver `MERGE` bruker `(zone, hour_start_utc)`. Kjøring av samme dato gir samme faktarader uten duplikater.
- Gold bygges deterministisk fra Silver etter en DQ-gate.
- `429`, `408` og `5xx` får begrenset eksponentiell retry med jitter. `4xx`, ugyldig JSON og schema-brudd logges i `ingestion_errors` og feiler raskt.
- Ugyldige time-rader isoleres i `quarantine_hourly`; de forsvinner ikke stille.

## Datakvalitet

`05_quality.py` skriver ett målbart resultat per regel med run-id, observasjon, terskel og alvorlighetsgrad. Reglene dekker blant annet:

- dupliserte eller manglende naturlige nøkler
- dupliserte time-nøkler i en ny kildepayload (quarantine før `MERGE`); prisfeil stopper Gold
- urealistiske temperatur-, vind-, nedbør- og prisverdier (negative strømpriser er tillatt)
- 23–25 priser per lokal dato/sone
- manglende værdekning i timesjoinen (warning under 95 %)
- ingestion-feil for samme run

For denne lille demoen kontrollerer timeregel cardinalitet. I produksjon ville jeg i tillegg generert en DST-korrekt forventet time-spine i `Europe/Oslo` og anti-joinet den mot kilde- og faktatabellene for å peke ut eksakt manglende tidspunkt – slik oppdager man også én manglende time som ellers kamufleres av en duplikat.

Free Edition-serverless avviser `CHECK`-constraints i denne workspace-konfigurasjonen. I stedet for å skjule det har denne demoen eksplisitte, testbare DQ-regler og en kvalitetssperre før Gold. I en produksjonsworkspace der policyen tillater det kan samme sentrale regler også håndheves som Delta-constraints.

## Produksjonstenkning utover demoen

`resources/weather_power_job.yml` viser en daglig Lakeflow Job med to parallelle ingest-oppgaver → Silver → DQ-gate → Gold. Den har `max_concurrent_runs=1`, task-timeout og pauset schedule. HTTP-klienten har avgrenset retry med backoff bare for transient 408/429/5xx; jeg har med hensikt ikke brukt generisk task-retry i demoen, siden den ikke kan skille en 4xx-/kontraktfeil fra en transient feil. I produksjon ville neste steg være varsling for jobbfail/SLA-brudd, en parameterisert lookback på siste 3–7 dager og en feilklassifiserende retry-policy; mottakergruppe og terskler er virksomhetsspesifikke og derfor ikke hardkodet her.

Unity Catalog-prinsipp: job-identiteten får minst mulig `USE CATALOG`, `USE SCHEMA`, `MODIFY` og `SELECT` der den trenger det; analytikere får kun `SELECT` på Gold. Tabellenes lineage blir da synlig i UC. Free Edition er en demonstrasjonsflate, ikke en produksjonsplattform.

## Lokal verifisering

Den Databricks-spesifikke koden må kjøres i en workspace, men API-kontrakt, dato-/DST-logikk og Python-syntaks kan kontrolleres lokalt:

```powershell
python -m pip install -r requirements-dev.txt
python -m compileall -q scripts src tests
python -m pytest -q
python scripts/acquire_raw_snapshots.py --date 2025-11-10 --out sample_data/raw_snapshots
```

Før deploy: kjør også `databricks bundle validate`. Snapshot-generatoren skal bare brukes når Free Edition-ressursen ikke har offentlig API-egress; den lagrer ikke hemmeligheter.

## Intervju

Se [demo-runbook.md](docs/demo-runbook.md) for en fem–åtte minutters demo og [interview-cheatsheet.md](docs/interview-cheatsheet.md) for korte svar på de sannsynlige oppfølgingsspørsmålene.
