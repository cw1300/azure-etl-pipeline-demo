"""
Integration tests for the sales analytics pipeline
"""
import pytest
import os
import json
import sqlite3
import pandas as pd
from etl.pipeline import SalesDataPipeline
from etl.transformations import DataTransformer
from etl.quality_checks import DataQualityManager

class TestSalesDataPipeline:

    @pytest.fixture
    def test_config(self, tmp_path):
        """Create a test configuration"""
        config = {
            "pipeline_name": "test_pipeline",
            "version": "1.0.0",
            "sources": [
                {
                    "name": "sales",
                    "type": "csv",
                    "path": str(tmp_path / "test_sales.csv")
                }
            ],
            "destinations": [
                {
                    "name": "sales_table",
                    "source": "sales",
                    "type": "database",
                    "table": "sales"
                }
            ],
            "quality_checks": {
                "enabled": True,
                "threshold": 0.95,
                "fail_on_error": False
            }
        }

        config_path = tmp_path / "test_config.json"
        with open(config_path, 'w') as f:
            json.dump(config, f)

        return str(config_path)

    @pytest.fixture
    def test_sales_data(self, tmp_path):
        """Create test sales data"""
        data = {
            'sale_id': [1, 2, 3, 4, 5],
            'product_id': [101, 102, 103, 101, 102],
            'customer_id': [201, 202, 203, 201, 202],
            'quantity': [2, 1, 3, 1, 2],
            'amount': [100.0, 50.0, 150.0, 50.0, 100.0],
            'sale_date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05']
        }

        df = pd.DataFrame(data)
        csv_path = tmp_path / "test_sales.csv"
        df.to_csv(csv_path, index=False)

        return str(csv_path)

    @pytest.fixture
    def test_database(self, tmp_path):
        """Create test database with schema"""
        db_path = tmp_path / "test_analytics.db"
        conn = sqlite3.connect(db_path)

        # Create simplified test schema
        schema = """
        CREATE TABLE sales (
            sale_id INTEGER PRIMARY KEY,
            product_id INTEGER,
            customer_id INTEGER,
            quantity INTEGER,
            amount DECIMAL(10, 2),
            sale_date DATE
        );

        CREATE TABLE pipeline_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_name VARCHAR(100),
            status VARCHAR(20),
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            records_processed INTEGER,
            error_message TEXT
        );

        CREATE TABLE data_quality_logs (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER,
            table_name VARCHAR(100),
            check_type VARCHAR(50),
            check_result BOOLEAN,
            details TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        conn.executescript(schema)
        conn.close()

        return str(db_path)

    def test_pipeline_initialization(self, test_config):
        """Test pipeline initialization"""
        pipeline = SalesDataPipeline(test_config)
        assert pipeline.config['pipeline_name'] == 'test_pipeline'
        assert isinstance(pipeline.transformer, DataTransformer)
        assert isinstance(pipeline.quality_manager, DataQualityManager)

    def test_pipeline_extract(self, test_config, test_sales_data):
        """Test data extraction"""
        pipeline = SalesDataPipeline(test_config)
        extracted_data = pipeline.extract()

        assert 'sales' in extracted_data
        assert len(extracted_data['sales']) == 5
        assert 'sale_id' in extracted_data['sales'].columns

    def test_pipeline_transform(self, test_config, test_sales_data):
        """Test data transformation"""
        pipeline = SalesDataPipeline(test_config)
        extracted_data = pipeline.extract()
        transformed_data = pipeline.transform(extracted_data)

        assert 'sales' in transformed_data
        assert 'year' in transformed_data['sales'].columns
        assert 'price_per_unit' in transformed_data['sales'].columns

    def test_pipeline_run(self, test_config, test_sales_data, test_database, monkeypatch):
        """Test complete pipeline run"""
        # Monkeypatch the db_path
        def mock_init(self, config_path):
            self.config = self._load_config(config_path)
            self.transformer = DataTransformer()
            self.quality_manager = DataQualityManager()
            self.db_path = test_database
            self.run_id = None

        monkeypatch.setattr(SalesDataPipeline, '__init__', mock_init)

        pipeline = SalesDataPipeline(test_config)
        result = pipeline.run()

        assert result == True

        # Check pipeline run was logged
        conn = sqlite3.connect(test_database)
        runs = pd.read_sql_query("SELECT * FROM pipeline_runs", conn)
        assert len(runs) == 1
        assert runs['status'][0] == 'COMPLETED'

        # Check data was loaded
        sales = pd.read_sql_query("SELECT * FROM sales", conn)
        assert len(sales) == 5

        conn.close()

    def test_pipeline_error_handling(self, test_config, monkeypatch):
        """Test pipeline error handling"""
        # Simulate an error during extraction
        def mock_extract(self):
            raise Exception("Simulated extraction error")

        monkeypatch.setattr(SalesDataPipeline, 'extract', mock_extract)

        pipeline = SalesDataPipeline(test_config)
        result = pipeline.run()

        assert result == False

    def test_quality_checks(self):
        """Test data quality checks"""
        quality_manager = DataQualityManager()

        # Test with valid data
        good_data = pd.DataFrame({
            'sale_id': [1, 2, 3],
            'product_id': [101, 102, 103],
            'customer_id': [201, 202, 203],
            'quantity': [1, 2, 3],
            'amount': [100.0, 200.0, 300.0],
            'sale_date': ['2024-01-01', '2024-01-02', '2024-01-03']
        })

        results = quality_manager.validate_data(good_data, 'sales')

        # Check all validations pass
        for check, result in results.items():
            assert result['passed'] == True

        # Test with bad data
        bad_data = pd.DataFrame({
            'sale_id': [1, 1, None],  # Duplicate and null
            'product_id': [101, 102, 103],
            'customer_id': [201, 202, 203],
            'quantity': [-1, 2, 3],  # Negative quantity
            'amount': [100.0, 0.0, 300.0],
            'sale_date': ['2024-01-01', '2024-01-02', '2030-01-03']  # Future date
        })

        results = quality_manager.validate_data(bad_data, 'sales')

        # Check some validations fail
        assert results['not_null_columns']['passed'] == False
        assert results['unique_columns']['passed'] == False
        assert results['numeric_ranges']['passed'] == False