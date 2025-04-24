"""
Unit tests for data transformations
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from etl.transformations import DataTransformer

class TestDataTransformer:

    @pytest.fixture
    def transformer(self):
        return DataTransformer()

    @pytest.fixture
    def sample_sales_data(self):
        return pd.DataFrame({
            'sale_id': [1, 2, 3],
            'product_id': [101, 102, 103],
            'customer_id': [201, 202, 203],
            'quantity': [2, 1, 3],
            'amount': [100.0, 50.0, 150.0],
            'sale_date': ['2024-01-01', '2024-01-02', '2024-01-03']
        })

    @pytest.fixture
    def sample_product_data(self):
        return pd.DataFrame({
            'product_id': [101, 102, 103],
            'product_name': ['Widget A', 'Widget B', 'Widget C'],
            'category': ['Electronics', 'Home', 'Electronics'],
            'price': [50.0, 50.0, 50.0]
        })

    @pytest.fixture
    def sample_customer_data(self):
        return pd.DataFrame({
            'customer_id': [201, 202, 203],
            'customer_name': ['John Doe', 'Jane Smith', 'Bob Johnson'],
            'email': ['john@gmail.com', 'jane@company.com', 'bob@yahoo.com'],
            'region': ['North', 'South', 'East']
        })

    def test_transform_sales_data(self, transformer, sample_sales_data):
        result = transformer.transform_sales_data(sample_sales_data)

        # Check that new columns are added
        assert 'year' in result.columns
        assert 'month' in result.columns
        assert 'quarter' in result.columns
        assert 'day_of_week' in result.columns
        assert 'is_weekend' in result.columns
        assert 'price_per_unit' in result.columns

        # Check date conversion
        assert pd.api.types.is_datetime64_any_dtype(result['sale_date'])

        # Check calculations
        assert result['price_per_unit'][0] == 50.0
        assert result['price_per_unit'][1] == 50.0
        assert result['price_per_unit'][2] == 50.0

    def test_transform_product_data(self, transformer, sample_product_data):
        result = transformer.transform_product_data(sample_product_data)

        # Check that new columns are added
        assert 'price_tier' in result.columns

        # Check text standardization
        assert result['category'][0] == 'ELECTRONICS'
        assert result['product_name'][0] == 'Widget A'

        # Check price tier assignment
        assert result['price_tier'][0] == 'Budget'

    def test_transform_customer_data(self, transformer, sample_customer_data):
        result = transformer.transform_customer_data(sample_customer_data)

        # Check that new columns are added
        assert 'email_domain' in result.columns
        assert 'customer_segment' in result.columns

        # Check email processing
        assert result['email_domain'][0] == 'gmail.com'
        assert result['email'][0] == 'john@gmail.com'

        # Check customer segmentation
        assert result['customer_segment'][0] == 'Personal'
        assert result['customer_segment'][1] == 'Other'

    def test_create_fact_table(self, transformer, sample_sales_data, sample_product_data, sample_customer_data):
        # Transform data first
        sales_transformed = transformer.transform_sales_data(sample_sales_data)
        products_transformed = transformer.transform_product_data(sample_product_data)
        customers_transformed = transformer.transform_customer_data(sample_customer_data)

        # Create fact table
        fact_table = transformer.create_fact_table(
            sales_transformed, 
            products_transformed,
            customers_transformed
        )

        # Check merged columns
        assert 'category' in fact_table.columns
        assert 'price_tier' in fact_table.columns
        assert 'region' in fact_table.columns
        assert 'customer_segment' in fact_table.columns
        assert 'revenue_category' in fact_table.columns
        assert 'date_key' in fact_table.columns

        # Check data integrity
        assert len(fact_table) == len(sales_transformed)
        assert fact_table['date_key'][0] == int(pd.to_datetime('2024-01-01').strftime('%Y%m%d'))

    def test_create_dimension_tables(self, transformer, sample_sales_data, sample_product_data, sample_customer_data):
        dimensions = transformer.create_dimension_tables(
            sample_sales_data, 
            sample_product_data, 
            sample_customer_data
        )

        # Check dimension tables exist
        assert 'dim_date' in dimensions
        assert 'dim_product' in dimensions
        assert 'dim_customer' in dimensions

        # Check date dimension structure
        date_dim = dimensions['dim_date']
        assert 'date_key' in date_dim.columns
        assert 'year' in date_dim.columns
        assert 'month' in date_dim.columns
        assert 'is_weekend' in date_dim.columns

        # Check product dimension structure
        product_dim = dimensions['dim_product']
        assert 'product_key' in product_dim.columns
        assert 'effective_date' in product_dim.columns
        assert 'is_current' in product_dim.columns

        # Check customer dimension structure
        customer_dim = dimensions['dim_customer']
        assert 'customer_key' in customer_dim.columns
        assert 'effective_date' in customer_dim.columns
        assert 'is_current' in customer_dim.columns