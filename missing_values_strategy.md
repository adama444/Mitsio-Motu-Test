# Stratégie de gestion des valeurs manquantes - Colonne nb_filles

## Contexte
Le fichier effectifs_eleves.csv contient les effectifs d'élèves par établissement, avec une distinction filles/garçons. La colonne nb_filles présente des valeurs manquantes pour certains établissements.

## Stratégie adoptée
- Seuil de tolérance : 5% de données manquantes

### Cas 1 : Taux de valeurs manquantes <= 5%
- Suppression pure des lignes avec nb_filles manquant
- Justification : À faible proportion, la suppression n'introduit pas de biais significatif et préserve l'intégrité des données originales. Cette approche est privilégiée car elle évite toute altération artificielle des effectifs.

### Cas 2 : Taux de valeurs manquantes > 5%
- Imputation par la médiane par établissement
- Justification : Au-delà de 5%, la suppression supprimerait trop d'observations, compromettant la représentativité des données. L'imputation par établissement respecte la structure des données (un établissement a généralement des effectifs stables dans le temps). La médiane est choisie plutôt que la moyenne car elle est robuste aux valeurs aberrantes.
- Fallback : Si un établissement n'a aucune valeur de nb_filles, la médiane globale de la colonne est utilisée.

## Considérations éthiques et statistiques:
- L'imputation altere effectivement les données originales. Cette altération est
  documentée et tracée dans les logs de traitement.
- La stratégie par établissement suppose une certaine homogénéité intra-établissement,
  hypothèse raisonnable dans le contexte éducatif.
- En cas d'analyse sensible (ex: inégalités filles/garçons), il est recommandé de
  comparer les résultats avec/sans imputation.

## Limites:
- L'imputation par médiane ne capture pas la variance intra-établissement
- Les établissements sans historique (aucune valeur) utilisent la médiane globale,
  ce qui peut masquer des spécificités locales

## Améliorations possibles:
- Utiliser un modèle prédictif
- Imputation multiple pour quantifier l'incertitude
- Marquage explicite des valeurs imputées dans une colonne dédiée
