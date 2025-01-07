# **ETL proces datasetu MovieLens**

Tento repozitár obsahuje implementáciu ETL procesu v Snowflake pre analýzu dát z **MovieLens** datasetu. Projekt sa zameriava na preskúmanie správania používateľov a ich sledovateľských preferencií na základe hodnotení filmov a demografických údajov používateľov. Výsledný dátový model umožňuje multidimenzionálnu analýzu a vizualizáciu kľúčových metrik.

---
## **1. Úvod a popis zdrojových dát**
Cieľom semestrálneho projektu je analyzovať dáta týkajúce sa filmov, používateľov a ich hodnotení. Táto analýza umožňuje identifikovať trendy vo sledovateľských preferenciách, najpopulárnejšie filmy a správanie používateľov.

Zdrojové dáta pochádzajú z GroupLens datasetu dostupného [tu](https://grouplens.org/datasets/movielens/).

Dataset obsahuje sedem hlavných tabuliek:
- `age_group`
- `genres`
- `movies`
- `occupations`
- `ratings`
- `tags`
- `users`

Účelom ETL procesu bolo tieto dáta pripraviť, transformovať a sprístupniť pre viacdimenzionálnu analýzu.

---
### **1.1 Dátová architektúra**

### **ERD diagram**
Surové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na **entitno-relačnom diagrame (ERD)**:

<p align="center">
  <img src="https://github.com/ErikSkultetyUKF/DT-MovieLens-ETL/blob/main/movielens_erd_schema.png" alt="ERD Schema">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma MovieLens</em>
</p>

---
## **2 Dimenzionálny model**

Navrhnutý bol **hviezdicový model (star schema)**, pre efektívnu analýzu kde centrálny bod predstavuje faktová tabuľka **`fact_ratings`**, ktorá je prepojená s nasledujúcimi dimenziami:
- **`dim_movies`**: Obsahuje podrobné informácie o filmoch (názov, rok vydania).
- **`dim_users`**: Obsahuje demografické údaje o používateľoch, ako sú vek, vekové kategórie, pohlavie, PSČ a povolanie.
- **`dim_tags`**: Obsahuje podrobné informácie o tag-och (tagy, dátum a čas vytvorenia).
- **`dim_genres`**: Obsahuje názvy pre kategórie filmov (žánre).
- **`dim_date`**: Zahrňuje informácie o dátumoch hodnotení (deň, mesiac, rok).
- **`dim_time`**: Obsahuje podrobné časové údaje (hodina, minúta, sekunda).

Štruktúra hviezdicového modelu je znázornená na diagrame nižšie. Diagram ukazuje prepojenia medzi faktovou tabuľkou a dimenziami, čo zjednodušuje pochopenie a implementáciu modelu.

<p align="center">
  <img src="https://github.com/ErikSkultetyUKF/DT-MovieLens-ETL/blob/main/movielens_star_schema.png" alt="Star Schema">
  <br>
  <em>Obrázok 2 Schéma hviezdy pre MovieLens</em>
</p>


---
## **3. ETL proces v Snowflake**
ETL proces pozostáva z troch hlavných fáz: `extrahovanie` (Extract), `transformácia` (Transform) a `načítanie` (Load). Tento proces bol implementovaný v Snowflake s cieľom pripraviť zdrojové dáta zo staging vrstvy do viacdimenzionálneho modelu vhodného na analýzu a vizualizáciu.

---
### **3.1 Extract (Extrahovanie dát)**
Dáta zo zdrojového datasetu (formát `.csv`) boli najprv nahraté do Snowflake prostredníctvom interného stage úložiska s názvom `movielens_stage`. Stage v Snowflake slúži ako dočasné úložisko na import alebo export dát. Vytvorenie stage bolo zabezpečené príkazom:

#### Príklad kódu:

```sql
CREATE OR REPLACE STAGE movielens_stage;
```

Pre prácu so súbormi datasetu bol špecificky vytvorený formát `movielens_csv`, pomocou podobného príkazu: 

#### Príklad kódu:

```sql
CREATE OR REPLACE FILE FORMAT MOVIELENS_CSV
TYPE = CSV
COMPRESSION = NONE
FIELD_DELIMITER = ','
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
FILE_EXTENSION = 'csv'
SKIP_HEADER = 1
RECORD_DELIMITER = '\n'
TRIM_SPACE = FALSE
NULL_IF = ('NULL', 'null', '');
```

Do stage boli následne nahraté súbory obsahujúce údaje o filmoch, žánroch, používateľoch, vekových skupinách, hodnoteniach, zamestnaniach a tagoch. Dáta boli importované do staging tabuliek pomocou príkazu `COPY INTO`. Pre každú tabuľku sa použil podobný príkaz:

#### Príklad kódu:

```sql
COPY INTO ratings_staging
FROM @SPARROW_MOVIELENS_STAGE/ratings.csv
FILE_FORMAT = MOVIELENS_CSV
```

V prípade nekonzistentných záznamov bol použitý parameter `ON_ERROR = 'CONTINUE'`, ktorý zabezpečil pokračovanie procesu bez prerušenia pri chybách.

---
### **3.2 Transform (Transformácia dát)**

V tejto fáze boli dáta zo staging tabuliek vyčistené, transformované a obohatené. Hlavným cieľom bolo pripraviť dimenzie a faktovú tabuľku, ktoré umožnia jednoduchú a efektívnu analýzu.

**Dimenzie boli navrhnuté na poskytovanie kontextu pre faktovú tabuľku.**

Dimenzia `dim_tags` obsahuje údaje o čase vrátane názvu a dátumu vytvorenia.

```sql
CREATE OR REPLACE TABLE dim_tags AS
SELECT
    ROW_NUMBER() OVER (ORDER BY tags) AS ID,
    tags,
    created_at
FROM tags_staging;
```

Dimenzia `dim_time` obsahuje údaje o čase vrátane hodiny, minúty a sekundy.

```sql
CREATE OR REPLACE TABLE dim_time AS
SELECT
    ROW_NUMBER() OVER (ORDER BY EXTRACT(HOUR FROM rated_at), EXTRACT(MINUTE FROM rated_at)) AS ID,
    EXTRACT(HOUR FROM rated_at) AS hour,
    EXTRACT(MINUTE FROM rated_at) AS minute,
    EXTRACT(SECOND FROM rated_at) AS second
FROM ratings_staging
GROUP BY EXTRACT(HOUR FROM rated_at), EXTRACT(MINUTE FROM rated_at), EXTRACT(SECOND FROM rated_at)
ORDER BY hour, minute, second;
```

Dimenzia `dim_date` obsahuje údaje o dátumoch vrátane dňa, mesiaca a roka.

```sql
CREATE OR REPLACE TABLE dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(rated_at AS DATE)) AS ID,
    CAST(rated_at AS DATE) AS date,
    EXTRACT(DAY FROM rated_at) AS day,
    EXTRACT(MONTH FROM rated_at) AS month,
    EXTRACT(YEAR FROM rated_at) AS year,
FROM (
    SELECT DISTINCT
        CAST(rated_at AS DATE) AS rated_at,
        EXTRACT(DAY FROM rated_at) AS day,
        EXTRACT(MONTH FROM rated_at) AS month,
        EXTRACT(YEAR FROM rated_at) AS year
    FROM ratings_staging
) unique_dates;
```

Dimenzia `dim_movies` obsahuje údaje o filmoch vrátane názvu.

```sql
CREATE OR REPLACE TABLE dim_movies AS
SELECT
    ROW_NUMBER() OVER (ORDER BY title) AS ID,
    title,
    release_year
FROM movies_staging;
```

Dimenzia `dim_genres` obsahuje údaje o žánroch vrátane názvu.

```sql
CREATE OR REPLACE TABLE dim_genres AS
SELECT
    ROW_NUMBER() OVER (ORDER BY name) AS ID,
    name
FROM genres_staging
GROUP BY name;
```

Dimenzia `dim_users` obsahuje údaje o používateľoch vrátane vekových kategórií, pohlavia a zamestnania.

```sql
CREATE OR REPLACE TABLE dim_users AS
SELECT
    u.userId AS ID,
    CASE 
        WHEN u.age < 18 THEN 'Under 18'
        WHEN u.age BETWEEN 18 AND 24 THEN '18-24'
        WHEN u.age BETWEEN 25 AND 34 THEN '25-34'
        WHEN u.age BETWEEN 35 AND 44 THEN '35-44'
        WHEN u.age BETWEEN 45 AND 54 THEN '45-54'
        WHEN u.age >= 55 THEN '55+'
        ELSE 'Unknown'
    END AS age_group_name,
    u.gender,
    o.name AS occupation_name,
FROM users_staging u
JOIN occupations_staging o ON u.occupationId = o.occupationId;
```

Faktová tabuľka `fact_ratings` obsahuje záznamy o hodnoteniach a prepojenia na všetky dimenzie. Obsahuje kľúčové metriky, ako je hodnota hodnotenia a časový údaj.

```sql
CREATE OR REPLACE TABLE fact_ratings AS
SELECT 
    r.ratingId AS ID,
    r.ratings,
    du.ID AS dim_users_ID,
    dt.ID AS dim_tags_ID,
    dm.ID AS dim_movies_ID,
    dg.ID AS dim_genres_ID,
    dtime.ID AS dim_time_ID,
    ddate.ID AS dim_date_ID
FROM ratings_staging r
JOIN dim_users du ON r.userId = du.ID
JOIN dim_movies dm ON r.movieId = dm.ID
LEFT JOIN tags_staging ts ON r.userId = ts.userId AND r.movieId = ts.movieId
LEFT JOIN dim_tags dt ON ts.tags = dt.tags
JOIN genres_movies_staging gm ON r.movieId = gm.movieId
JOIN dim_genres dg ON gm.genreId = dg.ID
JOIN dim_time dtime 
    ON EXTRACT(HOUR FROM r.rated_at) = dtime.hour
    AND EXTRACT(MINUTE FROM r.rated_at) = dtime.minute
    AND EXTRACT(SECOND FROM r.rated_at) = dtime.second
JOIN dim_date ddate ON CAST(r.rated_at AS DATE) = ddate.date;
```

---
### **3.3 Load (Načítanie dát)**

Po úspešnom vytvorení dimenzií a faktovej tabuľky boli dáta nahraté do finálnej štruktúry. Na záver boli staging tabuľky odstránené, aby sa optimalizovalo využitie úložiska:

```sql
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS age_group_staging;
DROP TABLE IF EXISTS users_staging;
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS genres_movies_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS tags_staging;
```

ETL proces v Snowflake umožnil spracovanie pôvodných dát z `.csv` formátu do viacdimenzionálneho modelu typu hviezda. Tento proces zahŕňal čistenie, obohacovanie a reorganizáciu údajov. Výsledný model umožňuje analýzu čitateľských preferencií a správania používateľov, pričom poskytuje základ pre vizualizácie a reporty.

---
## **4 Vizualizácia**

Dashboard obsahuje `5 vizualizácií`, ktoré poskytujú základný prehľad o kľúčových metrikách a trendoch týkajúcich sa filmov, používateľov a hodnotení. Tieto vizualizácie odpovedajú na dôležité otázky a umožňujú lepšie pochopiť správanie používateľov a ich preferencie.

<p align="center">
  <img src="https://github.com/ErikSkultetyUKF/DT-MovieLens-ETL/blob/main/movielens_dashboard.png" alt="Star Schema">
  <br>
  <em>Obrázok 3 Dashboard MovieLens Datasetu</em>
</p>

---
### **Graf 1: Top 10 filmov podľa počtu hodnotení**
Táto vizualizácia zobrazuje 10 filmov s najväčším počtom hodnotení. Umožňuje identifikovať najpopulárnejšie filmy medzi používateľmi.

```sql
SELECT 
    m.title,
    COUNT(fact_ratings.ID) AS total_ratings
FROM fact_ratings
JOIN dim_movies m ON m.ID = fact_ratings.dim_movies_ID
GROUP BY m.title
ORDER BY total_ratings DESC
LIMIT 10;
```

---
### **Graf 2: Top 10 žánrov filmov s najväčším počtom hodnotení**
Tento graf zobrazuje 10 žánrov filmov s najväčším počtom hodnotení. Umožňuje identifikovať najpopulárnejšie žánre filmov medzi používateľmi.

```sql
SELECT 
    g.name,
    COUNT(fact_ratings.ID) AS total_ratings
FROM fact_ratings
JOIN dim_genres g ON g.ID = fact_ratings.dim_genres_ID
GROUP BY g.name
ORDER BY total_ratings DESC
LIMIT 10;
```

---
### **Graf 3: Top 20 rokov vydania filmov s najväčším hodnotením**
Táto vizualizácia ukatzuje 20 rokov vydania filmov filmov s najväčším hodnotením. Umožňuje identifikovať najpopulárnejšie roky vydania filmov medzi používateľmi.

```sql
SELECT
    m.release_year,
    SUM(fact_ratings.ID) AS total_ratings
FROM fact_ratings
JOIN dim_movies m ON m.ID = fact_ratings.dim_movies_ID
GROUP BY m.release_year
ORDER BY total_ratings DESC
LIMIT 20;
```

---
### **Graf 4: Top 5 najčasteších hodín hodnotenia užívateľmi**
Tento graf ukazuje časový údaj (`hodiny`). Umožňuje identifikovať hodiny, v ktorých používatelia najčastejšie hodnotili filmy.

```sql
SELECT
    tm.hour,
    COUNT(fact_ratings.ID) AS total_ratings
FROM fact_ratings
JOIN dim_time tm ON tm.ID = fact_ratings.dim_time_ID
GROUP BY tm.hour
ORDER BY total_ratings DESC
LIMIT 5;
```

---
### **Graf 5: Počet hodnotení podľa vekovej kategórie**
Tento graf vizualizuje vekovú kategóriu. Umožňuje identifikovať vekové kategórie používateľov, ktoré najviac hodnotia filmy.

```sql
SELECT 
    u.age_group_name,
    COUNT(fact_ratings.ID) AS total_ratings
FROM fact_ratings
JOIN dim_users u ON u.ID = fact_ratings.dim_users_ID
GROUP BY u.age_group_name
ORDER BY total_ratings DESC;
```

Dashboard poskytuje komplexný pohľad na dáta, pričom zodpovedá dôležité otázky týkajúce sa sledovateľských preferencií a správania používateľov. Vizualizácie umožňujú jednoduchú interpretáciu dát a môžu byť využité na optimalizáciu odporúčacích systémov, marketingových stratégií a filmových služieb.

---
**Autor:** Erik Škultéty
