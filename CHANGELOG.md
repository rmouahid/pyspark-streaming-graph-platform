# Changelog

Toutes les évolutions notables du projet sont listées ici. Le format suit
[Keep a Changelog](https://keepachangelog.com/fr/1.1.0/) et le projet
utilise le [versionnage sémantique](https://semver.org/lang/fr/).

## [1.0.0] - 2026-11-22

Première version livrée.

### Fonctionnalités

- Simulateur d'événements e-commerce (vues, likes, achats) déposés en JSON
  dans `data/events`.
- Pipeline PySpark Structured Streaming : schéma explicite, fenêtres
  temporelles et agrégations par type d'action.
- Graphe utilisateurs / vendeurs / produits construit avec GraphFrames et
  exporté en CSV.
- Dashboard Streamlit rafraîchi automatiquement (graphe interactif pyvis,
  répartition des actions, achats par catégorie).
- Lancement de la plateforme complète en une commande avec `run.sh`.

### Fiabilité

- Événements du simulateur et exports CSV du graphe publiés de façon
  atomique ; le dashboard tolère les fichiers vides ou mal formés (#25, #26).
- Événements malformés écartés par le pipeline ; interpréteur des workers
  et fuseau de la session Spark fixés.
- `run.sh` démarre sous bash quel que soit le shell appelant (#27).

### Qualité et documentation

- Suite de tests pytest (simulateur, pipeline de streaming, graphe,
  dashboard, lancement) et CI GitHub Actions avec ruff (#18).
- `print` remplacés par le module `logging`, imports triés.
- README complet (prérequis, installation, lancement, tests), rapport
  technique et licence MIT (#17).

[1.0.0]: https://github.com/rmouahid/pyspark-streaming-graph-platform/releases/tag/v1.0.0
