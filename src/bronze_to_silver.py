"""
Pipeline Bronze → Silver
Standardisation des données brutes du Ministère de l'Éducation.
"""

from pathlib import Path
import pandas as pd
import logging
from datetime import datetime
import re

# Chemins par défaut (relatifs à la racine du projet)
RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
SILVER_DIR = Path(__file__).parent.parent / "data" / "silver"


def setup_logger(silver_dir: Path, script_name: str) -> logging.Logger:
    """Configure et retourne un logger pour le traitement des données.
    
    Args:
        silver_dir: Répertoire de destination pour les logs
        script_name: Nom du script appelant (ex: 'clean_etablissements', 'clean_effectifs')
        
    Returns:
        Logger configuré
    """
    import logging
    from datetime import datetime
    
    # Création du répertoire de logs
    log_dir = Path(__file__).parent.parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Nom du fichier de log avec timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f"{script_name}_{timestamp}.log"
    
    # Configuration du logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(script_name)
    logger.info(f"Logger initialisé pour {script_name}")
    logger.info(f"Fichier de log: {log_file}")
    
    return logger, log_dir


def _handle_missing_by_deletion(data: pd.DataFrame, logger: logging.Logger, log_dir: Path) -> pd.DataFrame:
    """Stratégie 1: Suppression des lignes avec valeurs manquantes sur nb_filles.
    
    Args:
        data: DataFrame à traiter
        logger: Logger pour les messages
        log_dir: Répertoire pour sauvegarder les logs
        
    Returns:
        DataFrame nettoyé sans les lignes avec nb_filles manquant
    """
    
    missing_rows = data[data['nb_filles'].isna()]
    missing_count = len(missing_rows)
    total_count = len(data)
    missing_rate = missing_count / total_count
    
    logger.info(f"Stratégie 1: Suppression pure (taux manquants: {missing_rate:.2%} <= 5%)")
    
    if missing_count > 0:
        # Sauvegarde des lignes supprimées
        deletion_file = log_dir / f"deleted_rows_clean_effectifs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        missing_rows.to_csv(deletion_file, index=False, encoding='utf-8-sig')
        
        logger.info(f"Lignes à supprimer: {missing_count}")
        logger.info(f"Fichier sauvegardé: {deletion_file}")
        
        # Ajout de métadonnées pour traçabilité
        missing_rows['date_suppression'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        missing_rows['motif_suppression'] = 'valeur_manquante_nb_filles'
        missing_rows.to_csv(deletion_file, index=False, encoding='utf-8-sig')
        
        # Suppression
        data_clean = data.dropna(subset=['nb_filles'])
        
        logger.info(f"Données après suppression: {len(data_clean)} lignes (perte de {missing_count} lignes)")
    else:
        logger.info("Aucune valeur manquante détectée, aucune suppression nécessaire")
        data_clean = data.copy()
    
    return data_clean


def _handle_missing_by_imputation(data: pd.DataFrame, logger: logging.Logger, log_dir: Path) -> pd.DataFrame:
    """Stratégie 2: Imputation des valeurs manquantes par la médiane par établissement.
    
    Args:
        data: DataFrame à traiter
        logger: Logger pour les messages
        log_dir: Répertoire pour sauvegarder les logs
        
    Returns:
        DataFrame avec valeurs imputées
    """
    
    missing_count = data['nb_filles'].isna().sum()
    missing_rate = missing_count / len(data)
    
    logger.info(f"Stratégie 2: Imputation par médiane par établissement (taux manquants: {missing_rate:.2%} > 5%)")
    
    # Sauvegarder les lignes avec valeurs manquantes AVANT imputation
    imputed_rows = data[data['nb_filles'].isna()].copy()
    imputed_file = log_dir / f"imputed_values_clean_effectifs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    # Calculer la médiane par établissement
    median_by_etab = data.groupby('code_etablissement')['nb_filles'].transform('median')
    
    # Identifier les établissements sans aucune valeur
    etabs_without_median = median_by_etab.isna()
    nb_etabs_sans_median = data.loc[etabs_without_median, 'code_etablissement'].nunique()
    
    if nb_etabs_sans_median > 0:
        logger.warning(f"{nb_etabs_sans_median} établissements sans aucune valeur de nb_filles")
    
    # Pour les établissements sans aucune valeur, utiliser la médiane globale
    global_median = data['nb_filles'].median()
    logger.info(f"Médiane globale de nb_filles: {global_median}")
    
    median_by_etab = median_by_etab.fillna(global_median)
    
    # Ajouter la valeur imputée pour traçabilité
    imputed_rows['valeur_imputee'] = median_by_etab[imputed_rows.index]
    imputed_rows['mediane_etablissement'] = median_by_etab[imputed_rows.index]
    imputed_rows['mediane_globale_fallback'] = global_median
    imputed_rows['etablissement_sans_median'] = etabs_without_median[imputed_rows.index]
    imputed_rows['date_imputation'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Sauvegarder la trace des imputations
    imputed_rows.to_csv(imputed_file, index=False, encoding='utf-8-sig')
    logger.info(f"Trace des imputations sauvegardée: {imputed_file}")
    logger.info(f"Valeurs imputées: {len(imputed_rows)}")
    
    # Imputation
    data_clean = data.copy()
    data_clean['nb_filles'] = data_clean['nb_filles'].fillna(median_by_etab)
    
    # Vérification post-imputation
    remaining_missing = data_clean['nb_filles'].isna().sum()
    if remaining_missing > 0:
        logger.warning(f"{remaining_missing} valeurs restent manquantes après imputation")
    
    # Log des statistiques avant/après
    logger.info(f"Statistiques avant imputation - Moyenne: {data['nb_filles'].mean():.2f}, Médiane: {data['nb_filles'].median():.2f}")
    logger.info(f"Statistiques après imputation - Moyenne: {data_clean['nb_filles'].mean():.2f}, Médiane: {data_clean['nb_filles'].median():.2f}")
    
    return data_clean


def normalize_region(region_str: str) -> str:
    """Normalise les noms de régions en utilisant des regex patterns.
    
    Args:
        region_str: Chaîne de caractères représentant le nom de la région
        
    Returns:
        Nom normalisé de la région
    """
    if pd.isna(region_str):
        return pd.NA

    region = str(region_str).lower().strip()
    
    # Supprimer les préfixes communs
    region = re.sub(r'^(r[ée]gion)\s+(de\s+)?', '', region)
    
    # Mapping regex -> région normalisée
    patterns = {
        r'kara': 'Kara',
        r'savanes?': 'Savanes',
        r'centrale|centre': 'Centrale',
        r'maritime': 'Maritime',
        r'plateaux?': 'Plateaux',
    }
    
    # Appliquer le premier pattern qui correspond
    for pattern, normalized in patterns.items():
        if re.search(pattern, region):
            return normalized
    
    # Si aucun pattern ne correspond, retourner la version title case
    return region.title()


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
    
    # Configuration du logging
    logger, _ = setup_logger(silver_dir, "clean_etablissements")
    
    accent_map = {
        'è': 'e', 
        'é': 'e', 
    }
    
    try:
        data = pd.read_csv(raw_dir / "etablissements_scolaires.csv", encoding="latin-1", sep=";")
    except Exception as e:
        logger.error(f"Erreur lors de la lecture du fichier {raw_dir / 'etablissements_scolaires.csv'}: {e}")
        raise

    data.columns = data.columns.str.lower().str.replace(" ", "_")
    
    # Remplace les caractères accentués dans les noms de colonnes
    data.columns = data.columns.str.translate(str.maketrans(accent_map))
    
    if "code_etablissement" in data.columns:
        duplicate_count = data.duplicated(subset="code_etablissement").sum()
        data = data.drop_duplicates(subset="code_etablissement", keep="first")
        logger.info(f"Suppression des doublons sur code_etablissement: {duplicate_count} doublons supprimés")
    
    if {"region", "prefecture", "date_creation"}.issubset(data.columns):
        data["region"] = data["region"].str.strip()
        data["prefecture"] = data["prefecture"].str.strip()
        data["date_creation"] = pd.to_datetime(data["date_creation"], format="%d/%m/%Y", errors="coerce")
        data['date_creation'] = data['date_creation'].dt.date
    
    silver_dir.mkdir(parents=True, exist_ok=True)
    
    data.to_parquet(silver_dir / "etablissements.parquet", index=False)
    logger.info(f"Fichier écrit: {silver_dir / 'etablissements.parquet'} ({len(data)} lignes)")


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
        
    Note:
        Stratégie de gestion des valeurs manquantes sur nb_filles :
        - Seuil de tolérance : 5% de données manquantes
        - Si taux <= 5% : Suppression des lignes avec nb_filles manquant
        - Si taux > 5% : Imputation par la médiane par établissement
        - Les lignes supprimées sont sauvegardées dans logs/supprimees_nb_filles.csv
    """
    numeric_cols = ["nb_garcons", "nb_filles"]
    etablissements_file = silver_dir / "etablissements.parquet"
    THRESHOLD = 0.05
    
    # Configuration du logging
    logger, log_dir = setup_logger(silver_dir, "clean_effectifs")
    
    try:
        data = pd.read_csv(raw_dir / "effectifs_eleves.csv", encoding="utf-8-sig", sep=",")
    except Exception as e:
        logger.error(f"Erreur lors de la lecture du fichier {raw_dir / 'effectifs_eleves.csv'}: {e}")
        raise

    data.columns = data.columns.str.lower().str.replace(" ", "_")
    
    for col in numeric_cols:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors="coerce")
    
    if etablissements_file.exists() and 'code_etablissement' in data.columns:
        df_etablissements = pd.read_parquet(etablissements_file)
        valid_codes = df_etablissements['code_etablissement'].unique()
        
        invalid_rows = data[~data['code_etablissement'].isin(valid_codes)]
        if len(invalid_rows) > 0:
            ref_file = log_dir / "effectifs_sans_etablissement.csv"
            invalid_rows.to_csv(ref_file, index=False, encoding='utf-8-sig')
            logger.warning(f"Intégrité référentielle: {len(invalid_rows)} lignes supprimées (code_etablissement invalide)")
            logger.info(f"Lignes supprimées sauvegardées dans {ref_file}")
        
        data = data[data['code_etablissement'].isin(valid_codes)]
        logger.info(f"Après intégrité référentielle: {len(data)} lignes conservées")
        
    # Gestion des valeurs manquantes sur nb_filles
    if 'nb_filles' in data.columns:
        missing_count = data['nb_filles'].isna().sum()
        missing_rate = missing_count / len(data)
        logger.info(f"Valeurs manquantes sur nb_filles: {missing_count} ({missing_rate:.2%})")
        
        if missing_rate <= THRESHOLD:
            # Stratégie 1: Suppression pure
            data = _handle_missing_by_deletion(data, logger, log_dir)
        else:
            # Stratégie 2: Imputation par médiane
            data = _handle_missing_by_imputation(data, logger, log_dir)
    
    for col in numeric_cols:
        if col in data.columns:
            data[col] = data[col].round().astype('Int64')
    
    silver_dir.mkdir(parents=True, exist_ok=True)
    data.to_parquet(silver_dir / "effectifs.parquet", index=False)
    logger.info(f"Fichier écrit: {silver_dir / 'effectifs.parquet'} ({len(data)} lignes)")


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
        
    Note:
        Stratégie de gestion des doublons sur matricule :
        - En cas de doublon entre titulaires et contractuels, on conserve le titulaire
        - Justification: Un enseignant titulaire a un statut plus stable et des informations
          plus complètes (ancienneté, grade, etc.)
    """
    
    etablissements_file = silver_dir / "etablissements.parquet"
    excel_file = raw_dir / "personnel_enseignant.xlsx"
    accent_map = {
        'è': 'e', 
        'é': 'e', 
    }
    
    # Configuration du logging
    logger, log_dir = setup_logger(silver_dir, "clean_enseignants")
    
    try:
        df_titulaires = pd.read_excel(excel_file, sheet_name="Titulaires")
        df_contractuels = pd.read_excel(excel_file, sheet_name="Contractuels")
    except Exception as e:
        logger.error(f"Erreur lors de la lecture du fichier {excel_file}: {e}")
        raise
    
    df_titulaires['statut'] = 'titulaire'
    df_contractuels['statut'] = 'contractuel'
    
    data = pd.concat([df_titulaires, df_contractuels], ignore_index=True)
    logger.info(f"Fusion des deux ongletsterminée: {len(data)} lignes au total")
    
    data.columns = data.columns.str.lower().str.replace(" ", "_")
    
    # Remplace les caractères accentués dans les noms de colonnes
    data.columns = data.columns.str.translate(str.maketrans(accent_map))
    
    logger.info("Conversion des dates au format ISO 8601")
    date_columns = [col for col in data.columns if 'date' in col]
        
    if date_columns:
        logger.info(f"Colonnes de dates détectées: {date_columns}")
        
        for date_col in date_columns:
            data[date_col] = pd.to_datetime(data[date_col], errors='coerce')
            
            # Convertir en string ISO 8601 pour éviter les problèmes de timezone
            data[date_col] = data[date_col].dt.strftime('%Y-%m-%d') if not data[date_col].isna().all() else data[date_col]
    else:
        logger.info("Aucune colonne de date détectée")
        
    if etablissements_file.exists() and 'code_etablissement' in data.columns:
        df_etablissements = pd.read_parquet(etablissements_file)
        valid_codes = df_etablissements['code_etablissement'].unique()
        
        invalid_rows = data[~data['code_etablissement'].isin(valid_codes)]
        if len(invalid_rows) > 0:
            ref_file = log_dir / "enseignants_sans_etablissement.csv"
            invalid_rows.to_csv(ref_file, index=False, encoding='utf-8-sig')
            logger.warning(f"Intégrité référentielle: {len(invalid_rows)} lignes supprimées (code_etablissement invalide)")
            logger.info(f"Lignes supprimées sauvegardées dans {ref_file}")
        
        data = data[data['code_etablissement'].isin(valid_codes)]
        logger.info(f"Après intégrité référentielle: {len(data)} lignes conservées")
    
    if 'matricule' in data.columns:
        logger.info("Gestion des doublons sur matricule")
        
        # Stratégie: conserver les titulaires en priorité
        # Créer un ordre de priorité: titulaire > contractuel
        statut_priority = {'titulaire': 1, 'contractuel': 2}
        data['_priority'] = data['statut'].map(statut_priority)
        
        # Trier par priorité (1 = plus haute priorité) puis garder la première occurrence
        data_sorted = data.sort_values('_priority')
        initial_count = len(data_sorted)
        data_clean = data_sorted.drop_duplicates(subset='matricule', keep='first')
        
        # Supprimer la colonne temporaire
        data_clean = data_clean.drop(columns=['_priority'])
        removed_count = initial_count - len(data_clean)
        
        # Log des doublons supprimés
        if removed_count > 0:
            duplicates_log = data[data.duplicated(subset='matricule', keep=False)]
            duplicates_log = duplicates_log[duplicates_log['statut'] == 'contractuel']
            if len(duplicates_log) > 0:
                log_file_duplicates = log_dir / "doublons_enseignants_supprimes.csv"
                duplicates_log.to_csv(log_file_duplicates, index=False, encoding='utf-8-sig')
                logger.info(f"Détail des doublons supprimés: {log_file_duplicates}")
        
        data = data_clean
    
    silver_dir.mkdir(parents=True, exist_ok=True)
    data.to_parquet(silver_dir / "enseignants.parquet", index=False)
    logger.info(f"Fichier écrit: {silver_dir / 'enseignants.parquet'} ({len(data)} lignes)")


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
        
    Note:
        Stratégie de gestion des valeurs non numériques dans les colonnes budget :
        - Les valeurs 'N/A', 'en attente', 'non communiqué' sont converties en NaN
        - Les NaN sont ensuite remplacés par 0 (imputation par zéro)
        - Justification: Un budget non communiqué est considéré comme nul pour
          les calculs agrégés et les comparaisons entre régions
    """
    logger, log_dir = setup_logger(silver_dir, "clean_budgets")
    
    excel_file = raw_dir / "budgets_regionaux.xlsx"
    accent_map = {
        'è': 'e', 
        'é': 'e', 
    }
    
    # Valeurs à considérer comme NaN pour les budgets
    non_numeric_values = [
        'N/A', 'n/a', 'NA', 'na',
        'en attente', 'en_attente', 'non communiqué', 'non_communique',
        'non communique'
    ]

    try:
        excel_file_obj = pd.ExcelFile(excel_file)
        sheet_names = excel_file_obj.sheet_names
        sheet_name = sheet_names[0]
        data = pd.read_excel(excel_file, sheet_name=sheet_name)
    except Exception as e:
        logger.error(f"Erreur lors de la lecture du fichier {excel_file}: {e}")
        raise
    
    data.columns = data.columns.str.lower().str.replace(" ", "_")
    
    # Remplace les caractères accentués dans les noms de colonnes
    data.columns = data.columns.str.translate(str.maketrans(accent_map))
            
    if 'region' in data.columns:
        logger.info("Normalisation des noms de régions (approche regex)")
        
        # Appliquer la normalisation
        data['region_normalisee'] = data['region'].apply(normalize_region)
        
        # Remplacer la colonne originale
        data['region'] = data['region_normalisee']
        data = data.drop(columns=['region_normalisee'])
        
        unique_after = data['region'].nunique()
        logger.info(f"  - Valeurs uniques après (Régions): {unique_after}")
    
    # Identification des colonnes budget
    budget_columns = []
    
    for col in data.columns:
        if "budget" in col:
            budget_columns.append(col)

    if budget_columns:
        for col in budget_columns:
            logger.info(f"Traitement de la colonne: {col}")
            # Remplacer les valeurs non numériques par NaN
            for non_num in non_numeric_values:
                data[col] = data[col].replace(non_num, pd.NA, regex=False)
                
            # Remplacer les chaînes vides par NaN
            data[col] = data[col].replace(['', ' '], pd.NA)
            
            # Conversion en float (les NaN restent NaN)
            data[col] = pd.to_numeric(data[col], errors='coerce')

            zero_filled = data[col].isna().sum()
            data[col] = data[col].fillna(0)
            logger.info(f"NaN remplacés par 0: {zero_filled}")
            
    silver_dir.mkdir(parents=True, exist_ok=True)
    data.to_parquet(silver_dir / "budgets.parquet", index=False)
    logger.info(f"Fichier écrit: {silver_dir / 'budgets.parquet'} ({len(data)} lignes)")
    

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
