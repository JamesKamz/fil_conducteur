import os
import requests
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def download_drive_csv(output_dir="/opt/airflow/data/raw"):
    # File ID for the CSV requested
    file_id = "1s-x76gQ-eoM5sqT2Hhcfn087Aw5D__hD"
    url = f"https://drive.google.com/uc?export=download&id={file_id}"
    
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "gdrive_data.csv")
    
    logging.info(f"Téléchargement du fichier depuis Google Drive (ID: {file_id})")
    
    try:
        session = requests.Session()
        response = session.get(url, stream=True)
        
        # Check for Google Drive virus scan warning
        token = None
        for key, value in response.cookies.items():
            if key.startswith('download_warning'):
                token = value
                break
                
        if token:
            logging.info("Avertissement de scan détecté, utilisation du token pour continuer...")
            response = session.get(url, params={'confirm': token}, stream=True)
            
        response.raise_for_status()
        
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(32768):
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
                    
        logging.info(f"Téléchargement terminé avec succès. Fichier sauvegardé dans: {output_path}")
        return output_path
        
    except Exception as e:
        logging.error(f"Erreur lors du téléchargement Google Drive: {str(e)}")
        raise

if __name__ == "__main__":
    download_drive_csv("/opt/airflow/data/raw")
