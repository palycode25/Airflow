# Airflow - Évaluation : Pipeline météo

## Description

Ce projet met en place un DAG Airflow qui automatise la récupération, la transformation et l'exploitation de données météo pour entraîner un modèle de prédiction de température.

## Architecture du pipeline

```
get_weather_data
    ├── transform_data_last_20  →  data.csv
    └── transform_data_all      →  fulldata.csv
                                        │
                                        ▼
                        TaskGroup: train_models
                    (LinearRegression, DecisionTreeRegressor,
                            RandomForestRegressor)
                                        │
                                        ▼
                              select_best_model
                            → best_model.pickle
```

## Étapes du DAG

1. **get_weather_data** : récupère les données météo en temps réel pour 3 villes (Paris, London, Washington) via l'API [OpenWeatherMap](https://openweathermap.org/), et les stocke en JSON dans `raw_files/`.
2. **transform_data_last_20** : concatène les 20 derniers fichiers JSON en un fichier `data.csv` (utilisé pour le dashboard de visualisation).
3. **transform_data_all** : concatène tous les fichiers JSON en un fichier `fulldata.csv` (utilisé pour l'entraînement des modèles).
4. **train_models** (TaskGroup) : entraîne en parallèle 3 modèles de régression et transmet leur score de validation croisée via XCom.
5. **select_best_model** : compare les scores des 3 modèles, sélectionne le meilleur, le réentraîne sur l'ensemble des données et le sauvegarde (`best_model.pickle`).

## Stack technique

- **Airflow** 2.8.1 (CeleryExecutor)
- **Docker Compose** pour l'orchestration des services (Postgres, Redis, Airflow, dashboard)
- **scikit-learn** pour les modèles de machine learning
- **pandas** pour la transformation des données

## Choix techniques

- Les informations sensibles (clé API) et paramétrables (liste des villes) sont stockées dans des **Variables Airflow**, et non en dur dans le code.
- Le DAG s'exécute **toutes les minutes** (`* * * * *`) afin d'alimenter le dashboard en continu.
- `catchup=False` pour éviter le rattrapage d'exécutions manquées.

## Fichiers

- `weather_dag.py` : définition complète du DAG
- `explications.md` : détail des choix d'implémentation

