"""Fixtures partagées pour les tests pytest."""

from pathlib import Path

import pandas as pd
import pytest


@pytest.fixture
def raw_dir() -> Path:
    """Chemin vers le dossier data/raw/."""
    return Path(__file__).parent.parent / "data" / "raw"


@pytest.fixture
def silver_dir() -> Path:
    """Chemin vers le dossier data/silver/."""
    return Path(__file__).parent.parent / "data" / "silver"


@pytest.fixture
def gold_dir() -> Path:
    """Chemin vers le dossier data/gold/."""
    return Path(__file__).parent.parent / "data" / "gold"


def load_parquet(path: Path) -> pd.DataFrame:
    """Charge un fichier Parquet et retourne un DataFrame pandas.

    Args:
        path: Chemin vers le fichier Parquet.

    Returns:
        DataFrame pandas chargé depuis le fichier Parquet.

    Raises:
        FileNotFoundError: Si le fichier n'existe pas.
    """
    if not path.exists():
        raise FileNotFoundError(f"Fichier Parquet introuvable : {path}")
    return pd.read_parquet(path)
