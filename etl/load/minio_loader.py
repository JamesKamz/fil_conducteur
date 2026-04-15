import os
import glob
import logging
from minio import Minio
from minio.error import S3Error

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_minio_client():
    # En environnement Docker, le host est 'minio:9000'. En local, 'localhost:9000'
    endpoint = os.getenv("MINIO_URL", "minio:9000").replace("http://", "")
    access_key = os.getenv("MINIO_ROOT_USER", "minio")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
    
    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )

def ensure_bucket_exists(client, bucket_name):
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logging.info(f"Bucket '{bucket_name}' créé avec succès.")
        else:
            logging.info(f"Bucket '{bucket_name}' existe déjà.")
    except S3Error as e:
        logging.error(f"Erreur lors de la vérification du bucket: {e}")
        raise

def upload_directory_to_minio(client, bucket_name, local_dir, s3_prefix):
    for root, dirs, files in os.walk(local_dir):
        for file in files:
            # Skip spark SUCCESS files
            if file.startswith("_"):
                continue
                
            local_path = os.path.join(root, file)
            # Récupérer le chemin relatif par rapport au répertoire local_dir
            relative_path = os.path.relpath(local_path, local_dir)
            s3_path = os.path.join(s3_prefix, relative_path).replace("\\", "/")
            
            try:
                client.fput_object(bucket_name, s3_path, local_path)
                logging.info(f"Fichier uploadé: {local_path} -> s3://{bucket_name}/{s3_path}")
            except S3Error as e:
                logging.error(f"Erreur d'upload pour {local_path}: {e}")

def run_loader(processed_dir="/opt/airflow/data/processed", bucket="silver"):
    client = get_minio_client()
    ensure_bucket_exists(client, bucket)
    
    # Pour chaque source de données dans processed/ (books, gdrive_data, users)
    if os.path.exists(processed_dir):
        for data_source in os.listdir(processed_dir):
            source_path = os.path.join(processed_dir, data_source)
            if os.path.isdir(source_path):
                logging.info(f"Chargement des données pour la source: {data_source}")
                upload_directory_to_minio(client, bucket, source_path, s3_prefix=data_source)
    else:
        logging.warning(f"Le dossier spécifié n'existe pas: {processed_dir}")

if __name__ == "__main__":
    run_loader()
