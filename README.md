# **ETL proces datasetu MovieLens**

Tento repozitár obsahuje implementáciu ETL procesu v Snowflake pre analýzu dát z **MovieLens** datasetu. Projekt sa zameriava na preskúmanie správania používateľov a ich čitateľských preferencií na základe hodnotení kníh a demografických údajov používateľov. Výsledný dátový model umožňuje multidimenzionálnu analýzu a vizualizáciu kľúčových metrik.

---
## **1. Úvod a popis zdrojových dát**
Cieľom semestrálneho projektu je analyzovať dáta týkajúce sa filmov, používateľov a ich hodnotení. Táto analýza umožňuje identifikovať trendy vo sledovateľských preferenciách, najpopulárnejšie filmy a správanie používateľov.

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
