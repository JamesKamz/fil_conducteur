import os
import glob
import pandas as pd

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, current_timestamp, regexp_replace
    PYSPARK_AVAILABLE = True
except Exception:
    PYSPARK_AVAILABLE = False

def get_spark_session():
    if not PYSPARK_AVAILABLE:
        return None

    return SparkSession.builder \
        .appName("ETL_Pipeline_Transformation") \
        .master("local[*]") \
        .getOrCreate()


def _ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def _fallback_write_csv(df, out_path):
    _ensure_dir(out_path)
    df.to_csv(os.path.join(out_path, "part-00000.csv"), index=False)


def _fallback_process_books(input_path, output_dir):
    if not os.path.exists(input_path):
        print(f"File {input_path} does not exist. Skipping books transform.")
        return

    df = pd.read_csv(input_path)
    if "price" in df.columns:
        df["price"] = (
            df["price"].astype(str).str.replace(r"[^0-9.]", "", regex=True)
        )
        df["price"] = pd.to_numeric(df["price"], errors="coerce")

    if "title" in df.columns:
        df = df[df["title"].notna() & (df["title"].astype(str).str.strip() != "")]

    df["processed_at"] = pd.Timestamp.utcnow().isoformat()
    out_path = os.path.join(output_dir, "books")
    _fallback_write_csv(df, out_path)
    print(f"Books processing done (fallback CSV): {out_path}")


def _fallback_process_gdrive_data(input_path, output_dir):
    if not os.path.exists(input_path):
        print(f"File {input_path} does not exist. Skipping gdrive transform.")
        return

    df = None
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            df = pd.read_csv(input_path, encoding=enc, on_bad_lines="skip")
            break
        except Exception:
            continue

    if df is None:
        print(f"Could not parse gdrive file as CSV: {input_path}. Skipping gdrive transform.")
        return

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = df.drop_duplicates()
    df["processed_at"] = pd.Timestamp.utcnow().isoformat()

    out_path = os.path.join(output_dir, "gdrive_data")
    _fallback_write_csv(df, out_path)
    print(f"GDrive data processing done (fallback CSV): {out_path}")


def _fallback_process_api_users(input_path, output_dir):
    if not os.path.exists(input_path):
        print(f"File {input_path} does not exist. Skipping api transform.")
        return

    df = pd.read_json(input_path)
    out = pd.DataFrame()
    out["user_id"] = df.get("id")
    out["name"] = df.get("name")
    out["username"] = df.get("username")
    out["email"] = df.get("email")
    out["city"] = df["address"].apply(lambda x: x.get("city") if isinstance(x, dict) else None) if "address" in df.columns else None
    out["company_name"] = df["company"].apply(lambda x: x.get("name") if isinstance(x, dict) else None) if "company" in df.columns else None
    out["processed_at"] = pd.Timestamp.utcnow().isoformat()

    out_path = os.path.join(output_dir, "users")
    _fallback_write_csv(out, out_path)
    print(f"API users data processing done (fallback CSV): {out_path}")

def process_books(spark, input_path, output_dir):
    if not os.path.exists(input_path):
        print(f"File {input_path} does not exist. Skipping books transform.")
        return
        
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    # Clean price column and convert to numeric
    # Assumes 'price' column looks like '51.77' after scraper cleaned '£' string, 
    # but let's make sure it's decimal
    df_clean = df.withColumn("price", regexp_replace(col("price"), "[^0-9.]", "").cast("double"))
    
    # Add metadata
    df_clean = df_clean.withColumn("processed_at", current_timestamp())
    
    # Clean rating if needed (rating is string e.g., 'Three')
    # Filter out empty titles
    df_clean = df_clean.filter(col("title").isNotNull() & (col("title") != ""))
    
    # Write to processed
    out_path = os.path.join(output_dir, "books")
    df_clean.write.mode("overwrite").parquet(out_path)
    print(f"Books processing done: {out_path}")


def process_gdrive_data(spark, input_path, output_dir):
    if not os.path.exists(input_path):
        print(f"File {input_path} does not exist. Skipping gdrive transform.")
        return
        
    # Read the CSV (we assume it's a CSV from the extract step)
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    # Generic transformations: column names to lower case, spaces to underscores
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.strip().lower().replace(" ", "_"))
        
    df = df.dropDuplicates()
    df = df.withColumn("processed_at", current_timestamp())
    
    out_path = os.path.join(output_dir, "gdrive_data")
    df.write.mode("overwrite").parquet(out_path)
    print(f"GDrive data processing done: {out_path}")

def process_api_users(spark, input_path, output_dir):
    if not os.path.exists(input_path):
        print(f"File {input_path} does not exist. Skipping api transform.")
        return
    
    # Read multiline JSON array
    df = spark.read.option("multiline", "true").json(input_path)
    
    # Select important fields and flatten somewhat
    # Schema varies, we pick generic fields assuming JSONPlaceholder /users
    df_clean = df.select(
        col("id").cast("string").alias("user_id"),
        col("name"),
        col("username"),
        col("email"),
        col("address.city").alias("city"),
        col("company.name").alias("company_name")
    )
    
    df_clean = df_clean.withColumn("processed_at", current_timestamp())
    
    out_path = os.path.join(output_dir, "users")
    df_clean.write.mode("overwrite").parquet(out_path)
    print(f"API users data processing done: {out_path}")

def run_transform(input_dir="/opt/airflow/data/raw", output_dir="/opt/airflow/data/processed"):
    if PYSPARK_AVAILABLE:
        print("Starting Spark Transformation Job...")
        spark = get_spark_session()

        # Process each source
        process_books(spark, os.path.join(input_dir, "books_scraped.csv"), output_dir)
        process_gdrive_data(spark, os.path.join(input_dir, "gdrive_data.csv"), output_dir)
        process_api_users(spark, os.path.join(input_dir, "users_api.json"), output_dir)

        spark.stop()
        print("Spark Transformation Job Completed.")
    else:
        print("PySpark not available, running pandas fallback transformation...")
        _fallback_process_books(os.path.join(input_dir, "books_scraped.csv"), output_dir)
        _fallback_process_gdrive_data(os.path.join(input_dir, "gdrive_data.csv"), output_dir)
        _fallback_process_api_users(os.path.join(input_dir, "users_api.json"), output_dir)
        print("Fallback transformation completed.")

if __name__ == "__main__":
    run_transform()
