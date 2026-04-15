# Projet fil conducteur : Pipeline ETL Industrialisé

## 1. Présentation du projet

Ce projet met en place un pipeline ETL industrialisé de bout en bout.

Objectif :

- extraire des donnees depuis plusieurs sources,
- transformer ces donnees,
- charger le resultat dans un stockage objet type data lakehouse,
- orchestrer automatiquement tout le flux.

Sources traitees :

- site web : https://books.toscrape.com/
- fichier CSV Google Drive
- API REST JSON

Technologies principales :

- Apache Airflow (orchestration)
- Apache Spark (transformation)
- MinIO (stockage objet S3 compatible)
- Docker Compose (industrialisation)

## 2. Architecture technique

Le DAG principal est `main_etl_pipeline` dans `dags/pipeline_dag.py`.

Ordre d'execution :

1. Extraction en parallele
2. Transformation
3. Chargement MinIO

Composants :

- `etl/extract/scraper.py` : extraction web BooksToScrape
- `etl/extract/gdrive_downloader.py` : telechargement CSV Google Drive
- `etl/extract/api_client.py` : collecte API utilisateurs
- `etl/transform/spark_processor.py` : transformations Spark (avec fallback pandas)
- `etl/load/minio_loader.py` : chargement des resultats dans MinIO

## 3. Pre-requis

- Docker
- Docker Compose

Optionnel selon machine :

- `sudo` pour les commandes Docker

## 4. Lancement avec Docker (sans environnement virtuel)

Depuis la racine du projet :

```bash
bash scripts/bootstrap_and_run.sh
```

Si Docker demande les droits admin :

```bash
sudo bash scripts/bootstrap_and_run.sh
```

Ce script fait automatiquement :

1. demarrage des conteneurs coeur (Postgres, MinIO, Spark)
2. initialisation Airflow (`airflow-init`)
3. demarrage Airflow
4. unpause + trigger du DAG `main_etl_pipeline`
5. affichage d'un resume de statut

## 5. Acces interfaces

- Airflow : http://localhost:8080 (admin / admin)
- MinIO Console : http://localhost:9001 (minio / minio123)

## 6. Verification du resultat

Dans Airflow :

- verifier que le run `main_etl_pipeline` est `success`
- verifier que les taches `extract_*`, `transform_spark`, `load_minio` sont `success`

Dans MinIO :

- ouvrir le bucket `silver`
- verifier la presence des objets charges (books, gdrive_data, users)

## 7. Tests

Les tests sont disponibles dans `tests/`.

Si vous voulez les executer sans environnement virtuel, utilisez Docker :

```bash
sudo docker compose run --rm airflow python -m pytest tests/
```
