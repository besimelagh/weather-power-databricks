# Demo-runbook (5–8 minutter)

## Før intervjuet

1. Kjør en kjent dato, for eksempel `2025-11-10`, helt til Gold.
2. Ha disse fanene klare: README/arkitektur, Bronze-tabell, Silver-fakta, DQ-resultater, Gold-tabell og `06_visualize.py`.
3. Kjør `SELECT * FROM workspace.weather_power_ops.data_quality_results ORDER BY checked_at DESC` én gang, slik at siste run er synlig.

## Forslag til fortelling

**0:00–0:45 – problem og valg**

«Jeg har laget en liten løsning som viser de samme egenskapene jeg ville krevd av en produksjonspipeline: sporbar rådata, idempotens, eksplisitt datakvalitet og korrekte tidsnøkler. Jeg avgrenser til NO1 og Oslo for å holde Free Edition-kjøringen liten.»

**0:45–2:00 – Bronze**

Åpne `api_payloads`. Pek på `payload_json`, `payload_sha256`, `request_url`, `http_status`, `ingested_at` og `run_id`.

«Jeg lagrer ikke bare utpakkede kolonner. Hvis leverandøren korrigerer et svar, kan jeg alltid forklare og reprosessere fra akkurat den råresponsen. Samme hash får ikke en ny Bronze-rad.»

**2:00–3:30 – Silver og tid**

Åpne `hourly_weather_price`. Pek på `hour_start_utc`, `weather_handling` og `join_status`.

«Pris-API-et bruker lokal tid med offset, mens værkilden leveres i UTC. Jeg normaliserer begge til UTC før join. Det er viktig på dagene med 23 eller 25 timer; jeg joiner aldri på en lokal klokkeslettstreng.»

**3:30–4:30 – kvalitet og manglende data**

Åpne `data_quality_results`.

«Her skiller jeg kritiske kontraktsbrudd fra warnings. Pris blir aldri imputert. En enkelt intern manglende værtime kan interpoleres for temperatur og vind, men er tydelig flagget; lengre hull og nedbør beholdes som `NULL`, ikke som null.»

**4:30–5:30 – Gold og visualisering**

Kjør/vis `06_visualize.py`.

«Dette er deskriptiv analyse, ikke kausalitet. Strømpris avhenger også av time på døgnet, sesong, hydrologi, kapasitet og utenlandske markeder. Temperaturbåndet er et lettlest første snitt, ikke en prediksjonsmodell.»

**5:30–7:30 – drift**

Åpne job-definisjonen eller pek på `resources/weather_power_job.yml`.

«I produksjon kjører ingestene parallelt, så Silver, en kvalitetssperre, og Gold. Jeg har maks én aktiv run, bounded retry kun på transient API-feil, varsler separat for jobbfail og datakvalitet, og en kort lookback for kildekorrigeringer.»

## Ikke gjør i demoen

- Ikke påstå realtid; kildene er daglige REST-API-er.
- Ikke påstå at Oslo-vær representerer hele NO1 perfekt.
- Ikke si at negative strømpriser er kvalitetsfeil.
- Ikke skjul eventuell Free Edition-API-begrensning. Forklar snapshot-sporet og hvordan produksjon løser nettverket.
