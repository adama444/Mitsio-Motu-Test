# Réponses — Test DE-01

## Question 1 — Row-Level Security (RLS)

> Dans un contexte multi-ministériel, chaque ministère ne doit voir que ses propres données dans les dashboards Superset. Comment implémenteriez-vous un mécanisme de Row-Level Security dans PostgreSQL pour garantir cet isolement ?

Je crée une clé dont je me servirai pour mettre en place le RLS par exemple ministère VARCHAR(100) NOT NULL.

## Question 2 — Data Contracts

> Le ministère modifie régulièrement le format de ses fichiers. Comment mettriez-vous en place un système de data contracts pour détecter ces changements en amont ?

Je mettrai en place un fichier central yaml.

## Question 3 — Choix techniques

> Pourquoi le format Parquet est-il préféré au CSV pour la couche Silver ? Citez au moins 3 avantages concrets.

- Stockage en column + compression: requetes analytiques plus rapides
- Preservation des types des column contrairement au CSV qui assimile la plupart du temps en string le type des column
- Le partionnement des données
