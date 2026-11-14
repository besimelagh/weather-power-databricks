# Intervju-cheatsheet

## Åpning på ca. 45 sekunder

> «Jeg har valgt å lage en bevisst liten løsning som viser de samme prinsippene jeg bruker i større dataplattformer: tydelige lag, sporbar rådata, eksplisitte kvalitetsregler og idempotente kjøringer. Fra Ruter har jeg erfaring med operasjonelle dataflyter og kvalitetssjekker i Databricks/Delta/Unity Catalog, og fra NRVA med tidsserier og datakvalitet. Derfor har jeg prioritert korrekt timesammenstilling, observability og driftsmessige trade-offs fremfor å hente store datamengder.»

## Sannsynlige spørsmål og korte svar

### Hvorfor Bronze → Silver → Gold?

«Bronze er et revisjonsspor, ikke en analyseflate. Silver er der jeg gjør dataene konforme, kvalitetssikret og gjenkjørbare på en stabil time-grain. Gold er små, forbruksklare tabeller, slik at dashboard og analytikere ikke trenger å tolke rådata på nytt.»

### Hvorfor Open-Meteo og Hvakosterstrømmen, ikke Frost/ENTSO-E?

«Jeg ville ha en demo uten personlig API-nøkkel og med komplett historikk, så jeg valgte to åpne, små JSON-API-er. Jeg valgte bevisst historiske værdata, ikke Yr-prognose, fordi analysen forklarer realiserte priser. I produksjon ville jeg vurdert Frost som norsk observasjonskilde og lagt `client_id` i Secrets; for pris ville jeg avklare om ENTSO-E skal være source of record. Kildevalget er en isolert adapter – lagene, kvalitetsreglene og tidsmodellen endres ikke.»

### Hvordan er løsningen idempotent?

«Jeg antar at API-kall og jobbretry er at-least-once. Derfor gjør jeg sinken deterministisk: råpayload hasjes og MERGE-es i Bronze, og Silver MERGE-er på naturlig nøkkel `(zone, hour_start_utc)`. Rerun gir dermed effektivt exactly-once-resultat i tabellene.»

### Hva gjør du ved dårlig eller manglende data?

«Jeg skiller mellom fail, warning og quarantine. Korrupte payloads eller prisnøkler som bryter kontrakten stopper prosesseringen. En manglende værtime kan gi warning og eventuelt begrenset, flagget interpolasjon. Jeg bytter aldri ukritisk en manglende verdi med null.»

### Hvordan orkestrerer og overvåker du dette?

«En Lakeflow Job med to parallelle ingest-tasker, avhengigheter mot Silver, DQ-gate og Gold. Jeg setter max concurrent runs til én, tidsgrense, retries med backoff bare for 429/5xx og varsling for feil og treghet. I tillegg har jeg DQ-resultater som egne Delta-data, slik at datafreshness og dekningsgrad kan overvåkes over tid.»

### Hva er Unity Catalog-historien?

«I produksjon har hvert miljø sin katalog og lagene egne schemas. Pipeline-identiteten får bare nødvendige `USE`, `SELECT` og `MODIFY`-privilegier; analytikere får `SELECT` på Gold. Da får vi lineage, audit og kontrollert tilgang uten at alle kan lese råpayloads.»

### Hvordan ville du satt opp CI/CD?

«Jeg versjonerer notebooks, tester og Asset Bundle i Git. Pull request kjører Python-/kontraktstester og bundle validation. Samme bundle deployes til dev, får en kort integrasjonstest mot en fast dato, og promoteres deretter med miljøspesifikk konfigurasjon – aldri med manuelt endret produksjonskode.»

### Hvorfor ikke streaming/Auto Loader?

«Dette er daglige REST-kilder. Å late som det er streaming gir ingen verdi. Jeg ville brukt Auto Loader hvis leverandøren landet filer i objektlagring; for HTTP-kall er en liten, planlagt batch-jobb mer robust og enklere å operere.»

### Hvordan brukte du AI?

«Jeg brukte AI som utviklingsassistent til boilerplate, testideer og dokumentasjon. Jeg verifiserte API-kontraktene og tidssemantikken selv, gjennomgikk all generert kode og skrev kvalitetspolicyen eksplisitt. Jeg delte ingen secrets eller virksomhetsdata med modellen. AI akselererte utkastet; ansvaret for arkitektur, validering og trade-offs er mitt.»

## Gode erfaringer å koble inn naturlig

- **Ruter:** operasjonell automatisering, Python/SQL, Databricks Lakehouse, Delta, Unity Catalog og Terraform/Azure DevOps.
- **Aker BP:** API- og hendelsesdrevne integrasjoner, OAuth/service principals og feilsøking av Databricks-jobber/workspace/notebook-tilganger.
- **NRVA/Baneservice:** tidsserier og datakvalitet – en sterk analogi for timesjoin, forsinkede data og huller.
- **ViT:** Data Vault og mange integrasjonskilder – forklarer hvorfor lineage og robust rålagring betyr noe når volumet øker.
