import pytest
from unittest.mock import MagicMock, patch

from etl.extract.api_client import fetch_api_data
from etl.extract.scraper import run_scraper


@patch("etl.extract.api_client.requests.get")
@patch("etl.extract.api_client.os.makedirs", side_effect=PermissionError("denied"))
def test_fetch_api_data_permission_error(mock_makedirs, mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = [{"id": 1, "name": "Test User"}]
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    with pytest.raises(PermissionError):
        fetch_api_data("/opt/airflow/data/raw")


@patch("etl.extract.scraper.requests.get")
@patch("etl.extract.scraper.os.makedirs", side_effect=PermissionError("denied"))
def test_run_scraper_permission_error(mock_makedirs, mock_get):
    html_content = b'''
    <html><body>
        <article class="product_pod">
            <h3><a title="A Light in the Attic">A Light in the Attic</a></h3>
            <div class="product_price">
                <p class="price_color">\xc2\xa351.77</p>
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

    with pytest.raises(PermissionError):
        run_scraper("/opt/airflow/data/raw")
