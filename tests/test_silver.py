"""Tests de validation de la couche Silver."""

from pathlib import Path

import pandas as pd
import pytest

from tests.conftest import load_parquet

REGIONS_VALIDES = {"Savanes", "Kara", "Centrale", "Plateaux", "Maritime"}


# ---------------------------------------------------------------------------
# Existence des fichiers
# ---------------------------------------------------------------------------


def test_etablissements_parquet_exists(silver_dir: Path) -> None:
    assert (silver_dir / "etablissements.parquet").exists(), (
        "Le fichier etablissements.parquet est absent de data/silver/"
    )


def test_effectifs_parquet_exists(silver_dir: Path) -> None:
    assert (silver_dir / "effectifs.parquet").exists(), (
        "Le fichier effectifs.parquet est absent de data/silver/"
    )


def test_enseignants_parquet_exists(silver_dir: Path) -> None:
    assert (silver_dir / "enseignants.parquet").exists(), (
        "Le fichier enseignants.parquet est absent de data/silver/"
    )


def test_budgets_parquet_exists(silver_dir: Path) -> None:
    assert (silver_dir / "budgets.parquet").exists(), (
        "Le fichier budgets.parquet est absent de data/silver/"
    )


# ---------------------------------------------------------------------------
# Schéma des colonnes
# ---------------------------------------------------------------------------


def test_etablissements_schema(silver_dir: Path) -> None:
    df = load_parquet(silver_dir / "etablissements.parquet")
    colonnes_attendues = {
        "code_etablissement",
        "nom_etablissement",
        "region",
        "prefecture",
        "type",
        "statut",
        "date_creation",
    }
    assert colonnes_attendues.issubset(set(df.columns)), (
        f"Colonnes manquantes : {colonnes_attendues - set(df.columns)}"
    )
    assert pd.api.types.is_string_dtype(df["code_etablissement"]), "code_etablissement doit être str"
    assert pd.api.types.is_string_dtype(df["region"]), "region doit être str"


def test_effectifs_schema(silver_dir: Path) -> None:
    df = load_parquet(silver_dir / "effectifs.parquet")
    colonnes_attendues = {
        "code_etablissement",
        "annee_scolaire",
        "nb_garcons",
        "nb_filles",
    }
    assert colonnes_attendues.issubset(set(df.columns)), (
        f"Colonnes manquantes : {colonnes_attendues - set(df.columns)}"
    )
    assert pd.api.types.is_integer_dtype(df["nb_garcons"]), "nb_garcons doit être int"
    assert pd.api.types.is_integer_dtype(df["nb_filles"]), "nb_filles doit être int"


def test_enseignants_schema(silver_dir: Path) -> None:
    df = load_parquet(silver_dir / "enseignants.parquet")
    colonnes_attendues = {
        "matricule",
        "nom",
        "prenom",
        "code_etablissement",
        "matiere",
        "date_prise_de_poste",
        "statut",
    }
    assert colonnes_attendues.issubset(set(df.columns)), (
        f"Colonnes manquantes : {colonnes_attendues - set(df.columns)}"
    )
    assert pd.api.types.is_string_dtype(df["matricule"]), "matricule doit être str"
    assert pd.api.types.is_string_dtype(df["statut"]), "statut doit être str"


def test_budgets_schema(silver_dir: Path) -> None:
    df = load_parquet(silver_dir / "budgets.parquet")
    colonnes_attendues = {
        "region",
        "annee",
        "budget_fonctionnement",
        "budget_investissement",
        "budget_total",
    }
    assert colonnes_attendues.issubset(set(df.columns)), (
        f"Colonnes manquantes : {colonnes_attendues - set(df.columns)}"
    )
    assert pd.api.types.is_integer_dtype(df["annee"]) or pd.api.types.is_float_dtype(
        df["annee"]
    ), "annee doit être numérique"


# ---------------------------------------------------------------------------
# Qualité des données
# ---------------------------------------------------------------------------


def test_etablissements_no_duplicate_code(silver_dir: Path) -> None:
    df = load_parquet(silver_dir / "etablissements.parquet")
    duplicates = df["code_etablissement"].duplicated().sum()
    assert duplicates == 0, (
        f"{duplicates} doublons trouvés sur code_etablissement — ils doivent être supprimés"
    )


def test_enseignants_no_duplicate_matricule(silver_dir: Path) -> None:
    df = load_parquet(silver_dir / "enseignants.parquet")
    duplicates = df["matricule"].duplicated().sum()
    assert duplicates == 0, (
        f"{duplicates} doublons trouvés sur matricule — les doublons inter-onglets doivent être gérés"
    )


def test_dates_iso_format(silver_dir: Path) -> None:
    df = load_parquet(silver_dir / "etablissements.parquet")
    assert pd.api.types.is_datetime64_any_dtype(
        df["date_creation"]
    ) or pd.api.types.is_object_dtype(df["date_creation"]), (
        "date_creation doit être un type datetime ou date, pas une string JJ/MM/AAAA"
    )
    # Si object, vérifier que ce sont bien des dates parsables ISO
    if df["date_creation"].dtype == object:
        try:
            pd.to_datetime(df["date_creation"], format="%Y-%m-%d")
        except Exception:
            pytest.fail("date_creation contient des valeurs non-ISO (format JJ/MM/AAAA non converti)")


def test_effectifs_numeric_clean(silver_dir: Path) -> None:
    df = load_parquet(silver_dir / "effectifs.parquet")
    assert pd.api.types.is_integer_dtype(df["nb_garcons"]), (
        "nb_garcons doit être un entier — vérifier que les espaces dans '1 245' ont été supprimés"
    )
    assert pd.api.types.is_integer_dtype(df["nb_filles"]), (
        "nb_filles doit être un entier — vérifier que les espaces dans '1 245' ont été supprimés"
    )


def test_budgets_numeric_clean(silver_dir: Path) -> None:
    df = load_parquet(silver_dir / "budgets.parquet")
    for col in ["budget_fonctionnement", "budget_investissement", "budget_total"]:
        nulls = df[col].isna().sum()
        assert nulls == 0, (
            f"{nulls} valeurs nulles dans {col} — les 'N/A' et 'en attente' doivent être gérés"
        )


def test_budgets_regions_normalized(silver_dir: Path) -> None:
    df = load_parquet(silver_dir / "budgets.parquet")
    regions_trouvees = set(df["region"].unique())
    assert regions_trouvees == REGIONS_VALIDES, (
        f"Régions trouvées : {regions_trouvees}\n"
        f"Attendu exactement : {REGIONS_VALIDES}\n"
        "Vérifier la normalisation des variations orthographiques (KARA, kara, Région de Kara...)"
    )


def test_enseignants_statut_column(silver_dir: Path) -> None:
    df = load_parquet(silver_dir / "enseignants.parquet")
    valeurs_attendues = {"titulaire", "contractuel"}
    valeurs_trouvees = set(df["statut"].unique())
    assert valeurs_trouvees.issubset(valeurs_attendues), (
        f"La colonne statut contient des valeurs inattendues : {valeurs_trouvees - valeurs_attendues}"
    )


# ---------------------------------------------------------------------------
# Intégrité référentielle
# ---------------------------------------------------------------------------


def test_effectifs_referential_integrity(silver_dir: Path) -> None:
    etablissements = load_parquet(silver_dir / "etablissements.parquet")
    effectifs = load_parquet(silver_dir / "effectifs.parquet")
    codes_valides = set(etablissements["code_etablissement"])
    codes_effectifs = set(effectifs["code_etablissement"])
    orphelins = codes_effectifs - codes_valides
    assert len(orphelins) == 0, (
        f"Codes établissement orphelins dans effectifs : {orphelins}\n"
        "Les lignes avec ETB-999, ETB-998 doivent avoir été supprimées"
    )


def test_enseignants_referential_integrity(silver_dir: Path) -> None:
    etablissements = load_parquet(silver_dir / "etablissements.parquet")
    enseignants = load_parquet(silver_dir / "enseignants.parquet")
    codes_valides = set(etablissements["code_etablissement"])
    codes_enseignants = set(enseignants["code_etablissement"])
    orphelins = codes_enseignants - codes_valides
    assert len(orphelins) == 0, (
        f"Codes établissement orphelins dans enseignants : {orphelins}"
    )
