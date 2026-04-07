# Test Technique DE-01 — Standardisation de données ministérielles

> **Programme** : PDAAP — Digitalisation des Administrations Publiques du Togo  
> **Poste** : Data Engineer Junior  
> **Durée** : 3 heures  
> **Validation** : `uv sync && uv run pytest`

---

## Contexte

Dans le cadre du PDAAP, chaque ministère transmet ses données dans des formats hétérogènes (CSV, Excel, encodages variés). En tant que Data Engineer, votre première mission est de **standardiser ces fichiers bruts en un format analytique exploitable** (couche Silver de l'architecture medallion), puis de **construire une vue agrégée** (couche Gold) dans DuckDB.

Vous travaillez sur les données du **Ministère fictif de l'Éducation et de la Formation Professionnelle**. Le ministère a transmis 4 fichiers de sources différentes, dans des formats et qualités variés.

---

## Fichiers sources (dossier `data/raw/`)

| Fichier | Format | Particularités |
|---|---|---|
| `etablissements_scolaires.csv` | CSV, encodage `latin-1`, séparateur `;` | Contient la liste des établissements scolaires (nom, code, région, préfecture, type : primaire/secondaire/lycée, statut : public/privé). Quelques doublons sur le code établissement. Dates au format `JJ/MM/AAAA`. |
| `effectifs_eleves.csv` | CSV, encodage `utf-8-sig` (BOM), séparateur `,` | Effectifs par établissement et par année scolaire (code_etablissement, annee_scolaire, nb_garcons, nb_filles). Certaines valeurs numériques contiennent des espaces (ex: `1 245`). Quelques lignes avec des codes établissement inexistants. |
| `personnel_enseignant.xlsx` | Excel, 3 onglets | Onglet 1 (`Titulaires`) : enseignants titulaires (nom, prénom, matricule, code_etablissement, matiere, date_prise_poste). Onglet 2 (`Contractuels`) : même structure mais avec une colonne `date_fin_contrat` en plus. Onglet 3 (`Métadonnées`) : légende des codes matières. Dates au format Excel natif. Certains matricules en double entre les deux onglets. |
| `budgets_regionaux.xlsx` | Excel, 1 onglet | Budget alloué par région et par année (region, annee, budget_fonctionnement, budget_investissement, budget_total). Certaines cellules budget contiennent du texte (`"N/A"`, `"en attente"`). La colonne `region` contient des variations orthographiques (ex: `Kara` / `KARA` / `kara` / `Région de Kara`). |

---

## Tâches

### Partie 1 — Normalisation Bronze → Silver (60% de la note)

Écrire un pipeline Python qui :

1. **Lit chaque fichier source** en détectant/gérant correctement l'encodage et le format.
2. **Nettoie et standardise** les données :
   - Nommage des colonnes en `snake_case`
   - Dates au format ISO 8601 (`YYYY-MM-DD`)
   - Suppression des doublons (stratégie à justifier)
   - Gestion des valeurs manquantes (stratégie à documenter)
   - Normalisation des chaînes (casse, espaces, accents dans les noms de région)
   - Conversion des types (numériques, dates, catégoriels)
3. **Écrit les fichiers nettoyés** dans `data/silver/` au format **Parquet**, un fichier par entité :
   - `etablissements.parquet`
   - `effectifs.parquet`
   - `enseignants.parquet` (titulaires + contractuels fusionnés, avec une colonne `statut`)
   - `budgets.parquet`

**Contrainte** : le code doit être modulaire (une fonction par étape ou par fichier source). Utiliser `pandas` ou `polars` (au choix).

### Partie 2 — Construction d'une table Gold dans DuckDB (25% de la note)

Écrire un script SQL (ou Python + SQL) qui :

1. **Charge les fichiers Parquet Silver** dans DuckDB.
2. **Construit une table Gold `synthese_regionale`** à l'aide de **CTE (Common Table Expressions)** qui agrège, pour chaque région et chaque année :
   - Le nombre total d'établissements
   - L'effectif total d'élèves (garçons + filles)
   - Le taux de féminisation (% de filles)
   - Le ratio élèves/enseignant
   - Le budget total
   - Le budget par élève

La requête doit être lisible et bien structurée grâce aux CTE — chaque CTE doit avoir un rôle clair et nommé explicitement.

3. **Exporte le résultat** dans `data/gold/synthese_regionale.parquet`.

### Partie 3 — Questions ouvertes (15% de la note)

Répondre dans un fichier `ANSWERS.md` :

**Question 1 — Row-Level Security (RLS)**

> Dans un contexte multi-ministériel, chaque ministère ne doit voir que ses propres données dans les dashboards Superset. Comment implémenteriez-vous un mécanisme de Row-Level Security dans PostgreSQL pour garantir cet isolement ? Décrivez l'architecture (rôles, policies, schémas) et les limites éventuelles de cette approche.

**Question 2 — Data Contracts**

> Le ministère modifie régulièrement le format de ses fichiers (ajout de colonnes, changement de noms, modification des types). Comment mettriez-vous en place un système de **data contracts** pour détecter ces changements en amont et éviter la casse du pipeline ? Quels outils ou patterns utiliseriez-vous ? Donnez un exemple concret appliqué aux fichiers de ce test.

**Question 3 — Choix techniques**

> Justifiez en quelques lignes : pourquoi le format Parquet est-il préféré au CSV pour la couche Silver ? Citez au moins 3 avantages concrets dans le contexte d'un pipeline analytique.

---

## Structure attendue du repo

```
de-01-standardisation/
├── pyproject.toml                  # Dépendances (pandas/polars, duckdb, pyarrow, pytest)
├── uv.lock
├── README.md                       # Ce fichier (énoncé)
├── ANSWERS.md                      # Réponses aux questions ouvertes (Partie 3)
├── data/
│   ├── raw/                        # Fichiers sources (fournis)
│   │   ├── etablissements_scolaires.csv
│   │   ├── effectifs_eleves.csv
│   │   ├── personnel_enseignant.xlsx
│   │   └── budgets_regionaux.xlsx
│   ├── silver/                     # Fichiers Parquet nettoyés (à produire)
│   └── gold/                       # Table Gold exportée (à produire)
├── src/
│   ├── __init__.py
│   ├── bronze_to_silver.py         # Pipeline de normalisation (Partie 1)
│   └── silver_to_gold.py           # Construction table Gold (Partie 2)
├── tests/
│   ├── __init__.py
│   ├── test_silver.py              # Tests de validation Silver (fournis)
│   └── test_gold.py                # Tests de validation Gold (fournis)
└── scripts/
    └── generate_data.py            # Script de génération des données fictives (interne, non fourni au candidat)
```

---

## Tests fournis (`tests/`)

Les tests `pytest` fournis valident :

**`test_silver.py`** :
- Existence des 4 fichiers Parquet dans `data/silver/`
- Schéma attendu (noms et types de colonnes) pour chaque fichier
- Absence de doublons sur les clés primaires (code_etablissement, matricule, etc.)
- Formats de dates valides (ISO 8601)
- Pas de valeurs nulles sur les colonnes obligatoires
- Intégrité référentielle : chaque `code_etablissement` dans `effectifs` et `enseignants` existe dans `etablissements`

**`test_gold.py`** :
- Existence du fichier `data/gold/synthese_regionale.parquet`
- Schéma attendu (region, annee, nb_etablissements, effectif_total, taux_feminisation, ratio_eleves_enseignant, budget_total, budget_par_eleve)
- Cohérence des calculs (ex: budget_par_eleve = budget_total / effectif_total)
- Pas de valeurs nulles ou négatives dans les métriques

---

## Critères d'évaluation

| Critère | Poids |
|---|---|
| Tests pytest passent (`uv sync && uv run pytest`) | 40% |
| Qualité du code (lisibilité, modularité, nommage, docstrings) | 15% |
| Gestion des cas limites (encodages, doublons, types, valeurs manquantes) | 15% |
| Structuration des CTE dans la requête Gold | 10% |
| Qualité des réponses aux questions ouvertes | 15% |
| Bonus : logging, gestion d'erreurs, tests supplémentaires | 5% |

---

## Stack autorisée

- **Python 3.11+**
- **pandas** ou **polars** (au choix du candidat)
- **duckdb** (lecture Parquet + construction Gold)
- **pyarrow** (écriture Parquet)
- **openpyxl** (lecture Excel)
- **chardet** (détection d'encodage, optionnel)
- **pytest**

Toute librairie supplémentaire est autorisée à condition de la justifier dans le README.

---

## Consignes

1. Cloner le repo et exécuter `uv sync` pour installer les dépendances.
2. Les fichiers sources sont dans `data/raw/`. Ne pas les modifier.
3. Implémenter le pipeline dans `src/bronze_to_silver.py` et `src/silver_to_gold.py`.
4. Répondre aux questions dans `ANSWERS.md`.
5. Valider avec `uv run pytest` — tous les tests doivent passer.
6. Pousser votre travail sur une branche et ouvrir une Pull Request.

**Bon courage !**
