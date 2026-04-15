import pytest
import os
from pyspark.sql import SparkSession
from etl.transform.spark_processor import process_books

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-pyspark-local-testing") \
        .getOrCreate()

def test_process_books(spark, tmpdir):
    input_dir = tmpdir.mkdir("raw")
    output_dir = tmpdir.mkdir("processed")
    
    csv_file = input_dir.join("books_scraped.csv")
    csv_file.write("title,price,availability,rating\nBook1,10.50,In stock,Three\n,5.50,In stock,One\n")
    
    process_books(spark, str(csv_file), str(output_dir))
    
    out_path = os.path.join(str(output_dir), "books")
    assert os.path.exists(out_path)
    
    df_out = spark.read.parquet(out_path)
    assert df_out.count() == 1  # Ligne sans titre filtrée
    assert "processed_at" in df_out.columns
