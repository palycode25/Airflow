import os
import json
import requests
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from joblib import dump


def get_weather_data():
    cities = Variable.get("cities", deserialize_json=True)
    api_key = Variable.get("openweathermap_api_key")

    results = []
    for city in cities:
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"
        response = requests.get(url)
        if response.status_code == 200:
            results.append(response.json())
        else:
            print(f"Erreur pour {city}: {response.status_code} - {response.text}")

    filename = datetime.now().strftime("%Y-%m-%d %H:%M") + ".json"
    filepath = os.path.join("/app/raw_files", filename)

    with open(filepath, "w") as f:
        json.dump(results, f)

    print(f"Fichier créé : {filepath}")


def transform_data_into_csv(n_files=None, filename='data.csv'):
    parent_folder = '/app/raw_files'
    files = sorted(os.listdir(parent_folder), reverse=True)
    if n_files:
        files = files[:n_files]

    dfs = []

    for f in files:
        with open(os.path.join(parent_folder, f), 'r') as file:
            data_temp = json.load(file)
        for data_city in data_temp:
            dfs.append(
                {
                    'temperature': data_city['main']['temp'],
                    'city': data_city['name'],
                    'pression': data_city['main']['pressure'],
                    'date': f.split('.')[0]
                }
            )

    df = pd.DataFrame(dfs)
    print('\n', df.head(10))
    df.to_csv(os.path.join('/app/clean_data', filename), index=False)


def prepare_data(path_to_data='/app/clean_data/fulldata.csv'):
    df = pd.read_csv(path_to_data)
    df = df.sort_values(['city', 'date'], ascending=True)

    dfs = []

    for c in df['city'].unique():
        df_temp = df[df['city'] == c]
        df_temp.loc[:, 'target'] = df_temp['temperature'].shift(1)

        for i in range(1, 10):
            df_temp.loc[:, 'temp_m-{}'.format(i)] = df_temp['temperature'].shift(-i)

        df_temp = df_temp.dropna()
        dfs.append(df_temp)

    df_final = pd.concat(dfs, axis=0, ignore_index=False)
    df_final = df_final.drop(['date'], axis=1)
    df_final = pd.get_dummies(df_final)

    features = df_final.drop(['target'], axis=1)
    target = df_final['target']

    return features, target


def compute_model_score(model, X, y):
    cross_validation = cross_val_score(
        model, X, y, cv=3, scoring='neg_mean_squared_error'
    )
    return cross_validation.mean()


def train_model_and_push_score(model, task_instance, xcom_key):
    X, y = prepare_data()
    score = compute_model_score(model, X, y)
    task_instance.xcom_push(key=xcom_key, value=score)
    print(f"Score pour {str(model)}: {score}")


def train_linear_regression(task_instance):
    train_model_and_push_score(LinearRegression(), task_instance, 'score_lr')


def train_decision_tree(task_instance):
    train_model_and_push_score(DecisionTreeRegressor(), task_instance, 'score_dt')


def train_random_forest(task_instance):
    train_model_and_push_score(RandomForestRegressor(), task_instance, 'score_rf')


def train_and_save_model(model, X, y, path_to_model='/app/clean_data/best_model.pickle'):
    model.fit(X, y)
    print(str(model), 'saved at', path_to_model)
    dump(model, path_to_model)


def select_best_model(task_instance):
    scores = {
        'score_lr': task_instance.xcom_pull(key='score_lr', task_ids='train_models.train_linear_regression'),
        'score_dt': task_instance.xcom_pull(key='score_dt', task_ids='train_models.train_decision_tree'),
        'score_rf': task_instance.xcom_pull(key='score_rf', task_ids='train_models.train_random_forest'),
    }

    print("Scores obtenus:", scores)

    best_model_key = max(scores, key=scores.get)

    models = {
        'score_lr': LinearRegression(),
        'score_dt': DecisionTreeRegressor(),
        'score_rf': RandomForestRegressor(),
    }

    best_model = models[best_model_key]
    print(f"Meilleur modèle: {best_model_key} avec score {scores[best_model_key]}")

    X, y = prepare_data()
    train_and_save_model(best_model, X, y)


with DAG(
    dag_id="weather_dag",
    description="Récupération et traitement de données météo",
    tags=["weather", "datascientest"],
    schedule_interval="* * * * *",
    default_args={
        "owner": "airflow",
        "start_date": days_ago(0, minute=1),
    },
    catchup=False
) as dag:

    task_1_get_weather = PythonOperator(
        task_id="get_weather_data",
        python_callable=get_weather_data
    )

    task_2_transform_last20 = PythonOperator(
        task_id="transform_data_last_20",
        python_callable=transform_data_into_csv,
        op_kwargs={"n_files": 20, "filename": "data.csv"}
    )

    task_3_transform_all = PythonOperator(
        task_id="transform_data_all",
        python_callable=transform_data_into_csv,
        op_kwargs={"n_files": None, "filename": "fulldata.csv"}
    )

    with TaskGroup("train_models") as train_models:
        task_4a_train_lr = PythonOperator(
            task_id="train_linear_regression",
            python_callable=train_linear_regression
        )

        task_4b_train_dt = PythonOperator(
            task_id="train_decision_tree",
            python_callable=train_decision_tree
        )

        task_4c_train_rf = PythonOperator(
            task_id="train_random_forest",
            python_callable=train_random_forest
        )

    task_5_select_best = PythonOperator(
        task_id="select_best_model",
        python_callable=select_best_model
    )

    task_1_get_weather >> [task_2_transform_last20, task_3_transform_all]
    task_3_transform_all >> train_models >> task_5_select_best
