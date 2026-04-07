"""
Pipeline Silver → Gold
Construction de la table de synthèse régionale dans DuckDB.
"""

from pathlib import Path

import duckdb

# Chemins par défaut (relatifs à la racine du projet)
SILVER_DIR = Path(__file__).parent.parent / "data" / "silver"
GOLD_DIR = Path(__file__).parent.parent / "data" / "gold"


def build_synthese_regionale(silver_dir: Path, gold_dir: Path) -> None:
    """Construit la table Gold synthese_regionale via DuckDB et l'exporte en Parquet.

    La requête doit utiliser des CTE (Common Table Expressions) pour agréger,
    par région et par année :
        - nb_etablissements  : nombre total d'établissements
        - effectif_total     : total garçons + filles
        - taux_feminisation  : ratio filles / effectif_total (entre 0 et 1)
        - ratio_eleves_enseignant : effectif_total / nb_enseignants
        - budget_total       : budget total de la région pour l'année
        - budget_par_eleve   : budget_total / effectif_total

    Le fichier de sortie est : gold_dir/synthese_regionale.parquet

    Exemple de structure CTE attendue :
    # WITH etablissements_par_region AS (
    #     SELECT region, annee, COUNT(*) as nb_etablissements
    #     FROM read_parquet('...')
    #     GROUP BY region, annee
    # ),
    # effectifs_par_region AS (
    #     SELECT e.region, ef.annee,
    #            SUM(ef.nb_garcons + ef.nb_filles) AS effectif_total,
    #            SUM(ef.nb_filles)::FLOAT / SUM(ef.nb_garcons + ef.nb_filles) AS taux_feminisation
    #     FROM read_parquet('...') ef
    #     JOIN read_parquet('...') e ON ef.code_etablissement = e.code_etablissement
    #     GROUP BY e.region, ef.annee
    # ),
    # enseignants_par_region AS (
    #     ...
    # ),
    # budgets AS (
    #     ...
    # )
    # SELECT
    #     e.region,
    #     e.annee,
    #     e.nb_etablissements,
    #     ef.effectif_total,
    #     ef.taux_feminisation,
    #     ef.effectif_total::FLOAT / ens.nb_enseignants AS ratio_eleves_enseignant,
    #     b.budget_total,
    #     b.budget_total::FLOAT / ef.effectif_total AS budget_par_eleve
    # FROM etablissements_par_region e
    # JOIN effectifs_par_region ef USING (region, annee)
    # JOIN enseignants_par_region ens USING (region, annee)
    # JOIN budgets b USING (region, annee)

    Args:
        silver_dir: Répertoire contenant les fichiers Parquet Silver.
        gold_dir: Répertoire de destination pour le fichier Gold.
    """
    raise NotImplementedError


if __name__ == "__main__":
    gold_dir = GOLD_DIR
    gold_dir.mkdir(parents=True, exist_ok=True)
    build_synthese_regionale(SILVER_DIR, gold_dir)
