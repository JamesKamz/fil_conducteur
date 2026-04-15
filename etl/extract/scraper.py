import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_scraper(output_dir="/opt/airflow/data/raw"):
    url = "https://books.toscrape.com/catalogue/category/books_1/page-1.html"
    books_data = []

    logging.info(f"Démarrage du scraping depuis: {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        books = soup.find_all('article', class_='product_pod')
        for book in books:
            title = book.h3.a['title']
            price_text = book.find('p', class_='price_color').text
            # Keep as string for now, will process in spark
            price = price_text.replace('£', '').strip() 
            availability = book.find('p', class_='instock availability').text.strip()
            rating = book.p['class'][1] # e.g., 'Three'
            
            books_data.append({
                'title': title,
                'price': price,
                'availability': availability,
                'rating': rating
            })
            
        logging.info(f"{len(books_data)} livres extraits avec succès.")
        
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "books_scraped.csv")
        df = pd.DataFrame(books_data)
        df.to_csv(output_path, index=False)
        logging.info(f"Données sauvegardées dans {output_path}")
        return output_path
        
    except Exception as e:
        logging.error(f"Erreur lors du scraping: {str(e)}")
        raise

if __name__ == "__main__":
    run_scraper("/opt/airflow/data/raw")
