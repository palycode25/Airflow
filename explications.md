# Explications - DAG weather_dag

## Architecture

Le DAG suit exactement le schéma demandé :

1. `get_weather_data` : récupère les données météo pour 3 villes (Paris, London, Washington) via l'API OpenWeatherMap et les stocke en JSON dans `/app/raw_files`
2. `transform_data_last_20` : concatène les 20 derniers fichiers JSON en `data.csv` (utilisé par le dashboard)
3. `transform_data_all` : concatène tous les fichiers JSON en `fulldata.csv` (utilisé pour l'entraînement)
4. TaskGroup `train_models` : entraîne en parallèle 3 modèles (LinearRegression, DecisionTreeRegressor, RandomForestRegressor) et transmet leur score de validation croisée via XCom
5. `select_best_model` : récupère les 3 scores, sélectionne le meilleur modèle, le réentraîne sur toutes les données et le sauvegarde (`best_model.pickle`)

## Choix techniques

- **Variables Airflow** : `cities` (liste des villes) et `openweathermap_api_key` (clé API) sont stockées en Variables plutôt qu'en dur dans le code, pour ne pas exposer la clé et faciliter les modifications sans toucher au code.
- **XCom** : chaque tâche d'entraînement pousse son score sous une clé distincte (`score_lr`, `score_dt`, `score_rf`), récupérées ensuite par `select_best_model` via `xcom_pull`.
- **TaskGroup** : les 3 tâches d'entraînement sont regroupées dans `train_models` pour une meilleure lisibilité du graphe, comme recommandé.
- **schedule_interval** : le DAG tourne toutes les minutes (`* * * * *`) afin d'alimenter le dashboard en continu.
- **catchup=False** : évite que les exécutions manquées (avant le lancement du DAG) soient rattrapées inutilement.

## Dépendances entre tâches

get_weather_data suivi de transform_data_last_20 et transform_data_all en parallèle.
transform_data_all suivi du TaskGroup train_models, lui-même suivi de select_best_model.
