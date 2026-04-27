"""
Pipeline Silver → Gold
Construction de la table de synthèse régionale dans DuckDB.
"""

from pathlib import Path

import duckdb
from src.bronze_to_silver import setup_logger

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
    etablissements_path = silver_dir / "etablissements.parquet"
    effectifs_path = silver_dir / "effectifs.parquet"
    enseignants_path = silver_dir / "enseignants.parquet"
    budgets_path = silver_dir / "budgets.parquet"
    
    # Connexion à DuckDB
    conn = duckdb.connect(':memory:')
    
    logger, log_dir = setup_logger(silver_dir, "clean_budgets")
    
    try:
        query = f"""
        -- CTE 1: Établissements par région et année scolaire
        WITH etablissements_par_region AS (
            SELECT 
                e.region,
                ef.annee_scolaire AS annee,
                COUNT(DISTINCT e.code_etablissement) AS nb_etablissements
            FROM read_parquet('{etablissements_path}') e
            INNER JOIN read_parquet('{effectifs_path}') ef
                ON e.code_etablissement = ef.code_etablissement
            WHERE e.region IS NOT NULL 
              AND ef.annee_scolaire IS NOT NULL
            GROUP BY e.region, ef.annee_scolaire
        ),
        
        -- CTE 2: Effectifs et taux de féminisation par région et année
        effectifs_par_region AS (
            SELECT 
                e.region,
                ef.annee_scolaire AS annee,
                SUM(COALESCE(ef.nb_garcons, 0) + COALESCE(ef.nb_filles, 0)) AS effectif_total,
                CASE 
                    WHEN SUM(COALESCE(ef.nb_garcons, 0) + COALESCE(ef.nb_filles, 0)) > 0
                    THEN SUM(COALESCE(ef.nb_filles, 0)) * 1.0 / 
                         SUM(COALESCE(ef.nb_garcons, 0) + COALESCE(ef.nb_filles, 0))
                    ELSE 0
                END AS taux_feminisation
            FROM read_parquet('{etablissements_path}') e
            INNER JOIN read_parquet('{effectifs_path}') ef
                ON e.code_etablissement = ef.code_etablissement
            WHERE e.region IS NOT NULL 
              AND ef.annee_scolaire IS NOT NULL
            GROUP BY e.region, ef.annee_scolaire
        ),
        
        -- CTE 3: Enseignants par région et année
        enseignants_par_region AS (
            SELECT 
                et.region,
                ens.annee_scolaire AS annee,
                COUNT(DISTINCT e.matricule) AS nb_enseignants
            FROM read_parquet('{enseignants_path}') e
            INNER JOIN read_parquet('{etablissements_path}') et
                 ON e.code_etablissement = et.code_etablissement
            INNER JOIN read_parquet('{effectifs_path}') ens
                ON e.code_etablissement = ens.code_etablissement
            WHERE et.region IS NOT NULL 
              AND ens.annee_scolaire IS NOT NULL
            GROUP BY et.region, ens.annee_scolaire
        ),
        
        -- CTE 4: Budgets par région et année
        budgets_par_region AS (
            SELECT 
                region,
                annee,
                SUM(COALESCE(budget_total, 0)) AS budget_total
            FROM read_parquet('{budgets_path}')
            WHERE region IS NOT NULL 
              AND annee IS NOT NULL
            GROUP BY region, annee
        ),
        
        -- CTE 5: Agrégation finale avec tous les indicateurs
        synthese_regionale AS (
            SELECT 
                e.region,
                e.annee,
                e.nb_etablissements,
                ef.effectif_total,
                ROUND(ef.taux_feminisation, 4) AS taux_feminisation,
                ens.nb_enseignants,
                -- Ratio élèves/enseignant (évite la division par zéro)
                CASE 
                    WHEN ens.nb_enseignants > 0 
                    THEN ROUND(ef.effectif_total * 1.0 / ens.nb_enseignants, 2)
                    ELSE NULL
                END AS ratio_eleves_enseignant,
                b.budget_total,
                -- Budget par élève (évite la division par zéro)
                CASE 
                    WHEN ef.effectif_total > 0 
                    THEN ROUND(b.budget_total * 1.0 / ef.effectif_total, 2)
                    ELSE NULL
                END AS budget_par_eleve
            FROM etablissements_par_region e
            INNER JOIN effectifs_par_region ef 
                ON e.region = ef.region AND e.annee = ef.annee
            INNER JOIN enseignants_par_region ens 
                ON e.region = ens.region AND e.annee = ens.annee
            INNER JOIN budgets_par_region b 
                ON e.region = b.region AND SPLIT_PART(e.annee, '-', 2) = b.annee
        )
        
        -- Sélection finale avec tri
        SELECT 
            region,
            annee,
            nb_etablissements,
            effectif_total,
            taux_feminisation,
            nb_enseignants,
            ratio_eleves_enseignant,
            budget_total,
            budget_par_eleve
        FROM synthese_regionale
        WHERE region IS NOT NULL AND annee IS NOT NULL
        ORDER BY region, annee
        """
        # Exécution de la requête
        logger.info("Exécution de la requête d'agrégation avec CTE...")
        result = conn.execute(query).fetchdf()
    
        logger.info(f"Table Gold construite: {len(result)} lignes, {len(result.columns)} colonnes")
        
        # Vérification que la table n'est pas vide
        if len(result) == 0:
            logger.warning("La table Gold est vide. Vérifiez les données sources.")
        
        # Export vers Parquet
        output_file = gold_dir / "synthese_regionale.parquet"
        result.to_parquet(output_file, index=False)
        
        logger.info(f"Export réussi: {output_file}")
    except duckdb.Error as e:
        logger.error(f"Erreur DuckDB: {e}")
        raise
    except Exception as e:
        logger.error(f"Erreur inattendue: {e}")
        raise
    finally:
        conn.close()
        logger.info("Connexion DuckDB fermée")


if __name__ == "__main__":
    gold_dir = GOLD_DIR
    gold_dir.mkdir(parents=True, exist_ok=True)
    build_synthese_regionale(SILVER_DIR, gold_dir)
