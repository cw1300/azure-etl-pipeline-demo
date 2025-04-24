"""
Data quality checks for the sales analytics pipeline
"""
import pandas as pd
import numpy as np
from loguru import logger

class DataQualityManager:
    """Manages data quality checks for the pipeline"""

    def __init__(self):
        self.rules = {
            'sales': {
                'required_columns': ['sale_id', 'product_id', 'customer_id', 'quantity', 'amount', 'sale_date'],
                'not_null_columns': ['sale_id', 'product_id', 'customer_id', 'sale_date'],
                'numeric_ranges': {
                    'quantity': {'min': 0, 'max': 1000},
                    'amount': {'min': 0, 'max': 1000000}
                },
                'unique_columns': ['sale_id']
            },
            'products': {
                'required_columns': ['product_id', 'product_name', 'category', 'price'],
                'not_null_columns': ['product_id', 'product_name'],
                'numeric_ranges': {
                    'price': {'min': 0, 'max': 10000}
                },
                'unique_columns': ['product_id']
            },
            'customers': {
                'required_columns': ['customer_id', 'customer_name', 'email', 'region'],
                'not_null_columns': ['customer_id', 'customer_name'],
                'unique_columns': ['customer_id', 'email']
            }
        }

    def validate_data(self, df, table_name):
        """Run all quality checks for a given dataframe"""
        logger.info(f"Running quality checks for {table_name}")

        results = {}
        if table_name in self.rules:
            rules = self.rules[table_name]

            # Check required columns
            results['required_columns'] = self._check_required_columns(df, rules.get('required_columns', []))

            # Check not null columns
            results['not_null_columns'] = self._check_not_null(df, rules.get('not_null_columns', []))

            # Check numeric ranges
            results['numeric_ranges'] = self._check_numeric_ranges(df, rules.get('numeric_ranges', {}))

            # Check unique columns
            results['unique_columns'] = self._check_unique_columns(df, rules.get('unique_columns', []))

            # Check data types
            results['data_types'] = self._check_data_types(df, table_name)

            # Run custom checks
            results['custom_checks'] = self._run_custom_checks(df, table_name)

        return results

    def _check_required_columns(self, df, required_columns):
        """Check if all required columns are present"""
        missing_columns = [col for col in required_columns if col not in df.columns]

        return {
            'passed': len(missing_columns) == 0,
            'details': {
                'missing_columns': missing_columns
            }
        }

    def _check_not_null(self, df, not_null_columns):
        """Check for null values in specified columns"""
        null_counts = {}
        for col in not_null_columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    null_counts[col] = int(null_count)

        return {
            'passed': len(null_counts) == 0,
            'details': {
                'null_counts': null_counts
            }
        }

    def _check_numeric_ranges(self, df, numeric_ranges):
        """Check if numeric values are within specified ranges"""
        out_of_range = {}
        for col, ranges in numeric_ranges.items():
            if col in df.columns:
                min_val = ranges.get('min', float('-inf'))
                max_val = ranges.get('max', float('inf'))

                count_below = (df[col] < min_val).sum()
                count_above = (df[col] > max_val).sum()

                if count_below > 0 or count_above > 0:
                    out_of_range[col] = {
                        'below_min': int(count_below),
                        'above_max': int(count_above)
                    }

        return {
            'passed': len(out_of_range) == 0,
            'details': {
                'out_of_range': out_of_range
            }
        }

    def _check_unique_columns(self, df, unique_columns):
        """Check for duplicate values in columns that should be unique"""
        duplicates = {}
        for col in unique_columns:
            if col in df.columns:
                duplicate_count = df[col].duplicated().sum()
                if duplicate_count > 0:
                    duplicates[col] = int(duplicate_count)

        return {
            'passed': len(duplicates) == 0,
            'details': {
                'duplicate_counts': duplicates
            }
        }

    def _check_data_types(self, df, table_name):
        """Check if data types are appropriate"""
        issues = []

        if table_name == 'sales':
            # Check if sale_date is datetime
            if 'sale_date' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['sale_date']):
                issues.append("sale_date should be datetime type")

            # Check if numeric columns are numeric
            numeric_cols = ['quantity', 'amount', 'price_per_unit']
            for col in numeric_cols:
                if col in df.columns and not pd.api.types.is_numeric_dtype(df[col]):
                    issues.append(f"{col} should be numeric type")

        return {
            'passed': len(issues) == 0,
            'details': {
                'issues': issues
            }
        }

    def _run_custom_checks(self, df, table_name):
        """Run custom data quality checks specific to each table"""
        issues = []

        if table_name == 'sales':
            # Check if quantity and amount are consistent
            if 'quantity' in df.columns and 'amount' in df.columns:
                zero_amount_with_quantity = ((df['quantity'] > 0) & (df['amount'] == 0)).sum()
                if zero_amount_with_quantity > 0:
                    issues.append(f"{zero_amount_with_quantity} records have quantity but zero amount")

            # Check for future dates
            if 'sale_date' in df.columns:
                future_dates = (pd.to_datetime(df['sale_date']) > pd.Timestamp.now()).sum()
                if future_dates > 0:
                    issues.append(f"{future_dates} records have future sale dates")

        elif table_name == 'products':
            # Check for zero or negative prices
            if 'price' in df.columns:
                invalid_prices = (df['price'] <= 0).sum()
                if invalid_prices > 0:
                    issues.append(f"{invalid_prices} products have zero or negative prices")

        elif table_name == 'customers':
            # Check email format
            if 'email' in df.columns:
                invalid_emails = (~df['email'].str.contains('@', na=False)).sum()
                if invalid_emails > 0:
                    issues.append(f"{invalid_emails} customers have invalid email formats")

        return {
            'passed': len(issues) == 0,
            'details': {
                'issues': issues
            }
        }

    def get_quality_report(self, results):
        """Generate a summary report of quality check results"""
        report = []
        total_checks = 0
        passed_checks = 0

        for check_type, result in results.items():
            total_checks += 1
            if result['passed']:
                passed_checks += 1
                report.append(f"✓ {check_type}: PASSED")
            else:
                report.append(f"✗ {check_type}: FAILED")
                report.append(f"  Details: {result['details']}")

        report.insert(0, f"Quality Check Summary: {passed_checks}/{total_checks} checks passed")
        return "\n".join(report)