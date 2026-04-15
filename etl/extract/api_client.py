import os
import requests
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_api_data(output_dir="/opt/airflow/data/raw"):
    url = "https://jsonplaceholder.typicode.com/users"
    
    logging.info(f"Récupération des données depuis l'API: {url}")
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        data = response.json()
        logging.info(f"{len(data)} utilisateurs récupérés.")
        
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "users_api.json")
        
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            
        logging.info(f"Données sauvegardées dans: {output_path}")
        return output_path
        
    except Exception as e:
        logging.error(f"Erreur lors de l'appel API: {str(e)}")
        raise

if __name__ == "__main__":
    fetch_api_data("/opt/airflow/data/raw")
