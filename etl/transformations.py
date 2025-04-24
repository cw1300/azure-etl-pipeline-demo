"""
Data transformation functions for the sales analytics pipeline
"""
import pandas as pd
import numpy as np
from datetime import datetime
from loguru import logger

class DataTransformer:
    """Handles data transformations for the pipeline"""

    def transform_sales_data(self, df):
        """Transform raw sales data"""
        logger.info("Transforming sales data")

        # Convert date strings to datetime
        df['sale_date'] = pd.to_datetime(df['sale_date'])

        # Add derived columns
        df['year'] = df['sale_date'].dt.year
        df['month'] = df['sale_date'].dt.month
        df['quarter'] = df['sale_date'].dt.quarter
        df['day_of_week'] = df['sale_date'].dt.dayofweek
        df['is_weekend'] = df['day_of_week'].isin([5, 6])

        # Calculate price per unit
        df['price_per_unit'] = df['amount'] / df['quantity']

        # Standardize numeric fields
        df['amount'] = df['amount'].round(2)
        df['price_per_unit'] = df['price_per_unit'].round(2)

        # Handle missing values
        df['quantity'] = df['quantity'].fillna(0)
        df['amount'] = df['amount'].fillna(0)

        # Add transformation timestamp
        df['transformed_at'] = datetime.now()

        return df

    def transform_product_data(self, df):
        """Transform raw product data"""
        logger.info("Transforming product data")

        # Standardize text fields
        df['product_name'] = df['product_name'].str.strip().str.title()
        df['category'] = df['category'].str.strip().str.upper()

        # Handle missing categories
        df['category'] = df['category'].fillna('UNCATEGORIZED')

        # Add price tiers
        df['price_tier'] = pd.cut(df['price'], 
                                  bins=[0, 50, 200, 500, float('inf')],
                                  labels=['Budget', 'Standard', 'Premium', 'Luxury'])

        # Add transformation timestamp
        df['transformed_at'] = datetime.now()

        return df

    def transform_customer_data(self, df):
        """Transform raw customer data"""
        logger.info("Transforming customer data")

        # Standardize text fields
        df['customer_name'] = df['customer_name'].str.strip().str.title()
        df['email'] = df['email'].str.strip().str.lower()
        df['region'] = df['region'].str.strip().str.upper()

        # Extract email domain
        df['email_domain'] = df['email'].str.split('@').str[1]

        # Add customer segments based on email domain
        df['customer_segment'] = df['email_domain'].apply(self._categorize_customer)

        # Handle missing regions
        df['region'] = df['region'].fillna('UNKNOWN')

        # Add transformation timestamp
        df['transformed_at'] = datetime.now()

        return df

    def _categorize_customer(self, email_domain):
        """Categorize customers based on email domain"""
        if not email_domain:
            return 'Unknown'

        corporate_domains = ['company.com', 'business.com', 'corp.com']
        personal_domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com']

        if email_domain in corporate_domains:
            return 'Corporate'
        elif email_domain in personal_domains:
            return 'Personal'
        else:
            return 'Other'

    def create_fact_table(self, sales_df, products_df, customers_df):
        """Create fact table by joining dimension tables"""
        logger.info("Creating fact table")

        # Merge sales with products
        fact_df = sales_df.merge(products_df[['product_id', 'category', 'price_tier']], 
                                on='product_id', 
                                how='left')

        # Merge with customers
        fact_df = fact_df.merge(customers_df[['customer_id', 'region', 'customer_segment']], 
                               on='customer_id', 
                               how='left')

        # Add additional metrics
        fact_df['revenue_category'] = pd.cut(fact_df['amount'],
                                           bins=[0, 100, 500, 1000, float('inf')],
                                           labels=['Small', 'Medium', 'Large', 'Enterprise'])

        # Add date key
        fact_df['date_key'] = fact_df['sale_date'].dt.strftime('%Y%m%d').astype(int)

        return fact_df

    def create_dimension_tables(self, sales_df, products_df, customers_df):
        """Create dimension tables for star schema"""
        logger.info("Creating dimension tables")

        # Date dimension
        date_dim = self._create_date_dimension(sales_df['sale_date'])

        # Product dimension (SCD Type 2)
        product_dim = self._create_product_dimension(products_df)

        # Customer dimension (SCD Type 2)
        customer_dim = self._create_customer_dimension(customers_df)

        return {
            'dim_date': date_dim,
            'dim_product': product_dim,
            'dim_customer': customer_dim
        }

    def _create_date_dimension(self, dates):
        """Create date dimension table"""
        unique_dates = pd.DataFrame({'full_date': dates.unique()})
        unique_dates['full_date'] = pd.to_datetime(unique_dates['full_date'])

        date_dim = pd.DataFrame({
            'date_key': unique_dates['full_date'].dt.strftime('%Y%m%d').astype(int),
            'full_date': unique_dates['full_date'],
            'year': unique_dates['full_date'].dt.year,
            'quarter': unique_dates['full_date'].dt.quarter,
            'month': unique_dates['full_date'].dt.month,
            'day': unique_dates['full_date'].dt.day,
            'day_of_week': unique_dates['full_date'].dt.dayofweek,
            'is_weekend': unique_dates['full_date'].dt.dayofweek.isin([5, 6])
        })

        return date_dim

    def _create_product_dimension(self, products_df):
        """Create product dimension with SCD Type 2"""
        product_dim = products_df.copy()
        product_dim['product_key'] = range(1, len(product_dim) + 1)
        product_dim['effective_date'] = datetime.now().date()
        product_dim['end_date'] = None
        product_dim['is_current'] = True

        return product_dim

    def _create_customer_dimension(self, customers_df):
        """Create customer dimension with SCD Type 2"""
        customer_dim = customers_df.copy()
        customer_dim['customer_key'] = range(1, len(customer_dim) + 1)
        customer_dim['effective_date'] = datetime.now().date()
        customer_dim['end_date'] = None
        customer_dim['is_current'] = True

        return customer_dim