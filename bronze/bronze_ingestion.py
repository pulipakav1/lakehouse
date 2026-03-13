import os
import sys
sys.path.insert(0, "/opt/airflow")

from datetime import datetime
from pyspark.sql import functions as F
from utils.config import (
    create_spark_session, get_logger,
    DATA_PATH, BRONZE_PATH, OLIST_FILES
)

logger = get_logger("bronze")


def ingest_to_bronze():
    logger.info("Starting Bronze ingestion — loading all 9 Olist CSVs")
    spark = create_spark_session("OlistBronzeIngestion")
    spark.sparkContext.setLogLevel("WARN")

    ingestion_ts = datetime.utcnow().isoformat()
    results = {}

    for table_name, filename in OLIST_FILES.items():
        filepath = os.path.join(DATA_PATH, filename)
        logger.info(f"Ingesting {table_name} from {filename}")

        try:
           
            df = (
                spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .option("encoding", "UTF-8")
                .csv(filepath)
            )

            row_count = df.count()

            df = (
                df
                .withColumn("_ingested_at",    F.lit(ingestion_ts))
                .withColumn("_source_file",    F.lit(filename))
                .withColumn("_ingestion_date", F.current_date())
            )

            # Write to S3 bronze 
            output_path = f"{BRONZE_PATH}/{table_name}"
            (
                df.write
                .format("parquet")
                .mode("overwrite")
                .save(output_path)
            )

            results[table_name] = row_count
            logger.info(f"✓ {table_name}: {row_count:,} rows → {output_path}")

        except Exception as e:
            logger.error(f" Failed to ingest {table_name}: {e}")
            raise

    spark.stop()

    # Summary
    total_rows = sum(results.values())
    logger.info(f"Bronze ingestion complete — {len(results)} tables, {total_rows:,} total rows")
    for table, count in results.items():
        logger.info(f"  {table:<25} {count:>10,} rows")

    return results


if __name__ == "__main__":
    ingest_to_bronze()
