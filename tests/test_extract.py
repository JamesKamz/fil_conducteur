import pytest
import os
from unittest.mock import patch, MagicMock
from etl.extract.api_client import fetch_api_data
from etl.extract.scraper import run_scraper

@patch("etl.extract.api_client.requests.get")
def test_fetch_api_data(mock_get, tmpdir):
    # Mocking la réponse de l'API
    mock_response = MagicMock()
    mock_response.json.return_value = [{"id": 1, "name": "Test User"}]
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    output_dir = str(tmpdir)
    output_path = fetch_api_data(output_dir)
    
    assert os.path.exists(output_path)
    mock_get.assert_called_once()

@patch("etl.extract.scraper.requests.get")
def test_run_scraper(mock_get, tmpdir):
    # Mock de contenu HTML
    html_content = b'''
    <html><body>
        <article class="product_pod">
            <h3><a title="A Light in the Attic">A Light in the Attic</a></h3>
            <div class="product_price">
                <p class="price_color">£51.77</p>
                <p class="instock availability">In stock</p>
            </div>
            <p class="star-rating Three"></p>
        </article>
    </body></html>
    '''
    mock_response = MagicMock()
    mock_response.content = html_content
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    output_dir = str(tmpdir)
    output_path = run_scraper(output_dir)
    
    assert os.path.exists(output_path)
    mock_get.assert_called_once()
