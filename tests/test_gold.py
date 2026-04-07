"""Tests de validation de la couche Gold."""

from pathlib import Path

import pandas as pd
import pytest

from tests.conftest import load_parquet

REGIONS_VALIDES = {"Savanes", "Kara", "Centrale", "Plateaux", "Maritime"}
COLONNES_GOLD = {
    "region",
    "annee",
    "nb_etablissements",
    "effectif_total",
    "taux_feminisation",
    "ratio_eleves_enseignant",
    "budget_total",
    "budget_par_eleve",
}


# ---------------------------------------------------------------------------
# Existence
# ---------------------------------------------------------------------------


def test_synthese_regionale_exists(gold_dir: Path) -> None:
    assert (gold_dir / "synthese_regionale.parquet").exists(), (
        "Le fichier synthese_regionale.parquet est absent de data/gold/"
    )


# ---------------------------------------------------------------------------
# Schéma
# ---------------------------------------------------------------------------


def test_synthese_regionale_schema(gold_dir: Path) -> None:
    df = load_parquet(gold_dir / "synthese_regionale.parquet")
    assert COLONNES_GOLD.issubset(set(df.columns)), (
        f"Colonnes manquantes : {COLONNES_GOLD - set(df.columns)}"
    )
    assert pd.api.types.is_integer_dtype(df["nb_etablissements"]) or pd.api.types.is_float_dtype(
        df["nb_etablissements"]
    ), "nb_etablissements doit être numérique"
    assert pd.api.types.is_float_dtype(df["taux_feminisation"]), (
        "taux_feminisation doit être float"
    )
    assert pd.api.types.is_float_dtype(df["ratio_eleves_enseignant"]) or pd.api.types.is_float_dtype(
        df["ratio_eleves_enseignant"]
    ), "ratio_eleves_enseignant doit être float"
    assert pd.api.types.is_float_dtype(df["budget_par_eleve"]), (
        "budget_par_eleve doit être float"
    )


# ---------------------------------------------------------------------------
# Cohérence des calculs
# ---------------------------------------------------------------------------


def test_budget_par_eleve_coherent(gold_dir: Path) -> None:
    df = load_parquet(gold_dir / "synthese_regionale.parquet")
    budget_recalcule = df["budget_total"] / df["effectif_total"]
    diff = (df["budget_par_eleve"] - budget_recalcule).abs()
    assert (diff < 0.01).all(), (
        "budget_par_eleve ne correspond pas à budget_total / effectif_total "
        f"(différence max : {diff.max():.4f})"
    )


def test_taux_feminisation_range(gold_dir: Path) -> None:
    df = load_parquet(gold_dir / "synthese_regionale.parquet")
    assert (df["taux_feminisation"] >= 0).all() and (df["taux_feminisation"] <= 1).all(), (
        "taux_feminisation doit être un ratio entre 0 et 1 (pas un pourcentage entre 0 et 100)"
    )


def test_no_null_metrics(gold_dir: Path) -> None:
    df = load_parquet(gold_dir / "synthese_regionale.parquet")
    colonnes_metriques = [
        "nb_etablissements",
        "effectif_total",
        "taux_feminisation",
        "ratio_eleves_enseignant",
        "budget_total",
        "budget_par_eleve",
    ]
    for col in colonnes_metriques:
        nulls = df[col].isna().sum()
        assert nulls == 0, f"La colonne {col} contient {nulls} valeur(s) nulle(s)"


def test_no_negative_metrics(gold_dir: Path) -> None:
    df = load_parquet(gold_dir / "synthese_regionale.parquet")
    colonnes_metriques = [
        "nb_etablissements",
        "effectif_total",
        "taux_feminisation",
        "ratio_eleves_enseignant",
        "budget_total",
        "budget_par_eleve",
    ]
    for col in colonnes_metriques:
        negatifs = (df[col] < 0).sum()
        assert negatifs == 0, f"La colonne {col} contient {negatifs} valeur(s) négative(s)"


def test_synthese_has_all_regions(gold_dir: Path) -> None:
    df = load_parquet(gold_dir / "synthese_regionale.parquet")
    regions_trouvees = set(df["region"].unique())
    manquantes = REGIONS_VALIDES - regions_trouvees
    assert len(manquantes) == 0, (
        f"Régions manquantes dans synthese_regionale : {manquantes}"
    )
