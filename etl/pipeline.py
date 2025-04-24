"""
Sales Data Pipeline - Core ETL functionality
"""
import pandas as pd
import sqlite3
import json
from datetime import datetime
from loguru import logger
from etl.transformations import DataTransformer
from etl.quality_checks import DataQualityManager

class SalesDataPipeline:
    """Metadata-driven ETL pipeline for sales data processing"""

    def __init__(self, config_path):
        self.config = self._load_config(config_path)
        self.transformer = DataTransformer()
        self.quality_manager = DataQualityManager()
        self.db_path = 'sales_analytics.db'
        self.run_id = None

    def _load_config(self, path):
        """Load pipeline configuration from JSON file"""
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {str(e)}")
            raise

    def _start_pipeline_run(self):
        """Initialize a new pipeline run and return run_id"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO pipeline_runs (pipeline_name, status, start_time)
            VALUES (?, ?, ?)
        """, (self.config['pipeline_name'], 'STARTED', datetime.now()))

        self.run_id = cursor.lastrowid
        conn.commit()
        conn.close()

        logger.info(f"Started pipeline run: {self.run_id}")
        return self.run_id

    def _end_pipeline_run(self, status, records_processed, error_message=None):
        """Update pipeline run with completion status"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE pipeline_runs 
            SET status = ?, end_time = ?, records_processed = ?, error_message = ?
            WHERE run_id = ?
        """, (status, datetime.now(), records_processed, error_message, self.run_id))

        conn.commit()
        conn.close()

        logger.info(f"Completed pipeline run {self.run_id} with status: {status}")

    def extract(self):
        """Extract data from sources defined in config"""
        extracted_data = {}

        for source in self.config['sources']:
            logger.info(f"Extracting from source: {source['name']}")

            try:
                if source['type'] == 'csv':
                    df = pd.read_csv(source['path'])
                elif source['type'] == 'database':
                    conn = sqlite3.connect(self.db_path)
                    df = pd.read_sql_query(source['query'], conn)
                    conn.close()
                else:
                    raise ValueError(f"Unsupported source type: {source['type']}")

                extracted_data[source['name']] = df
                logger.info(f"Extracted {len(df)} records from {source['name']}")

            except Exception as e:
                logger.error(f"Extraction failed for {source['name']}: {str(e)}")
                raise

        return extracted_data

    def transform(self, extracted_data):
        """Apply transformations to extracted data"""
        transformed_data = {}

        for source_name, df in extracted_data.items():
            logger.info(f"Transforming data from: {source_name}")

            # Apply specific transformations based on source
            if source_name == 'sales':
                df = self.transformer.transform_sales_data(df)
            elif source_name == 'products':
                df = self.transformer.transform_product_data(df)
            elif source_name == 'customers':
                df = self.transformer.transform_customer_data(df)

            # Apply data quality checks
            quality_results = self.quality_manager.validate_data(df, source_name)
            self._log_quality_results(quality_results, source_name)

            transformed_data[source_name] = df
            logger.info(f"Transformed {len(df)} records for {source_name}")

        return transformed_data

    def load(self, transformed_data):
        """Load transformed data to destination"""
        total_records = 0
        conn = sqlite3.connect(self.db_path)

        try:
            for destination in self.config['destinations']:
                source_name = destination['source']
                table_name = destination['table']

                if source_name in transformed_data:
                    df = transformed_data[source_name].copy()

                    # Select only the columns that exist in the target table
                    if table_name == 'sales':
                        df = df[['sale_id', 'product_id', 'customer_id', 'quantity', 'amount', 'sale_date']]
                    elif table_name == 'products':
                        df = df[['product_id', 'product_name', 'category', 'price']]
                    elif table_name == 'customers':
                        df = df[['customer_id', 'customer_name', 'email', 'region']]

                    # Load to staging table first
                    df.to_sql(f'stg_{table_name}', conn, if_exists='replace', index=False)

                    # Perform merge/upsert operation (simplified for SQLite)
                    if destination.get('merge_key'):
                        self._merge_data(conn, table_name, destination['merge_key'])
                    else:
                        df.to_sql(table_name, conn, if_exists='append', index=False)

                    total_records += len(df)
                    logger.info(f"Loaded {len(df)} records to {table_name}")

            conn.commit()

        except Exception as e:
            conn.rollback()
            logger.error(f"Load failed: {str(e)}")
            raise
        finally:
            conn.close()

        return total_records

    def _merge_data(self, conn, table_name, merge_key):
        """Merge data from staging to target table"""
        cursor = conn.cursor()

        cursor.execute(f"PRAGMA table_info(stg_{table_name})")
        columns = [row[1] for row in cursor.fetchall()]

        # Create a column list string
        columns_str = ', '.join(columns)

        # Simple upsert logic for SQLite
        cursor.execute(f"""
            INSERT OR REPLACE INTO {table_name} ({columns_str})
            SELECT {columns_str} FROM stg_{table_name}
        """)

    def _log_quality_results(self, results, table_name):
        """Log data quality check results"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        for check_type, result in results.items():
            cursor.execute("""
                INSERT INTO data_quality_logs (run_id, table_name, check_type, check_result, details)
                VALUES (?, ?, ?, ?, ?)
            """, (self.run_id, table_name, check_type, result['passed'], json.dumps(result['details'])))

        conn.commit()
        conn.close()

    def run(self):
        """Execute the complete ETL pipeline"""
        try:
            self._start_pipeline_run()

            # Extract
            extracted_data = self.extract()

            # Transform
            transformed_data = self.transform(extracted_data)

            # Load
            records_processed = self.load(transformed_data)

            self._end_pipeline_run('COMPLETED', records_processed)

            return True

        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            self._end_pipeline_run('FAILED', 0, str(e))
            return False