"""
Pipeline Bronze → Silver
Standardisation des données brutes du Ministère de l'Éducation.
"""

from pathlib import Path

import pandas as pd

# Chemins par défaut (relatifs à la racine du projet)
RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
SILVER_DIR = Path(__file__).parent.parent / "data" / "silver"


def clean_etablissements(raw_dir: Path, silver_dir: Path) -> None:
    """Nettoie le fichier etablissements_scolaires.csv et écrit etablissements.parquet.

    Opérations attendues :
    - Lire le CSV (encodage latin-1, séparateur ';')
    - Renommer les colonnes en snake_case
    - Supprimer les doublons sur code_etablissement (conserver la première occurrence)
    - Nettoyer les espaces dans les noms de région et de préfecture
    - Convertir la colonne date_creation du format JJ/MM/AAAA vers un type date Python
    - Écrire le résultat dans silver_dir/etablissements.parquet

    Args:
        raw_dir: Répertoire contenant les fichiers sources bruts.
        silver_dir: Répertoire de destination pour les fichiers Parquet nettoyés.
    """
    raise NotImplementedError


def clean_effectifs(raw_dir: Path, silver_dir: Path) -> None:
    """Nettoie le fichier effectifs_eleves.csv et écrit effectifs.parquet.

    Opérations attendues :
    - Lire le CSV (encodage utf-8-sig avec BOM, séparateur ',')
    - Renommer les colonnes en snake_case si nécessaire
    - Nettoyer les valeurs numériques contenant des espaces (ex: '1 245' → 1245)
    - Supprimer les lignes dont le code_etablissement n'existe pas dans etablissements.parquet
      (intégrité référentielle)
    - Gérer les valeurs manquantes sur nb_filles (stratégie à documenter)
    - Convertir nb_garcons et nb_filles en entiers
    - Écrire le résultat dans silver_dir/effectifs.parquet

    Args:
        raw_dir: Répertoire contenant les fichiers sources bruts.
        silver_dir: Répertoire de destination pour les fichiers Parquet nettoyés.
    """
    raise NotImplementedError


def clean_enseignants(raw_dir: Path, silver_dir: Path) -> None:
    """Nettoie le fichier personnel_enseignant.xlsx et écrit enseignants.parquet.

    Opérations attendues :
    - Lire les onglets 'Titulaires' et 'Contractuels'
    - Ajouter une colonne 'statut' ('titulaire' ou 'contractuel')
    - Fusionner les deux onglets dans un seul DataFrame
    - Supprimer les doublons inter-onglets sur matricule (stratégie à justifier)
    - Renommer les colonnes en snake_case
    - Convertir les dates au format ISO 8601
    - Supprimer les lignes dont le code_etablissement n'existe pas dans etablissements.parquet
    - Écrire le résultat dans silver_dir/enseignants.parquet

    Args:
        raw_dir: Répertoire contenant les fichiers sources bruts.
        silver_dir: Répertoire de destination pour les fichiers Parquet nettoyés.
    """
    raise NotImplementedError


def clean_budgets(raw_dir: Path, silver_dir: Path) -> None:
    """Nettoie le fichier budgets_regionaux.xlsx et écrit budgets.parquet.

    Opérations attendues :
    - Lire le fichier Excel (1 onglet)
    - Renommer les colonnes en snake_case
    - Normaliser la colonne 'region' : harmoniser les variations orthographiques
      (ex: 'KARA', 'kara', 'Région de Kara' → 'Kara')
    - Gérer les cellules budget non numériques ('N/A', 'en attente', 'non communiqué')
      → convertir en NaN ou en 0 (stratégie à documenter)
    - Convertir les colonnes budget en float
    - Écrire le résultat dans silver_dir/budgets.parquet

    Args:
        raw_dir: Répertoire contenant les fichiers sources bruts.
        silver_dir: Répertoire de destination pour les fichiers Parquet nettoyés.
    """
    raise NotImplementedError


def run_pipeline() -> None:
    """Exécute l'ensemble du pipeline Bronze → Silver.

    Appelle séquentiellement les 4 fonctions de nettoyage.
    Les fichiers Parquet sont écrits dans data/silver/.
    """
    silver_dir = SILVER_DIR
    silver_dir.mkdir(parents=True, exist_ok=True)

    clean_etablissements(RAW_DIR, silver_dir)
    clean_effectifs(RAW_DIR, silver_dir)
    clean_enseignants(RAW_DIR, silver_dir)
    clean_budgets(RAW_DIR, silver_dir)


if __name__ == "__main__":
    run_pipeline()
