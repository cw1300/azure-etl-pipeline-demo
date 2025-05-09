"""
Main entry point for the Sales Analytics Pipeline
"""
import os
import sqlite3
from etl.pipeline import SalesDataPipeline
from config.logging_config import setup_logging
from faker import Faker
import pandas as pd
import random

# Setup logging
logger = setup_logging()

def setup_database():
    """Initialize the database with schema and sample data"""
    logger.info("Setting up database...")

    # Create database connection
    conn = sqlite3.connect('sales_analytics.db')

    # Read and execute schema
    with open('database/schema.sql', 'r') as f:
        conn.executescript(f.read())

    # Read and execute stored procedures
    stored_proc_files = [
        'database/stored_procedures/sp_calculate_sales_metrics.sql',
        'database/stored_procedures/sp_dynamic_report.sql',
        'database/stored_procedures/sp_data_quality_check.sql'
    ]

    for proc_file in stored_proc_files:
        if os.path.exists(proc_file):
            with open(proc_file, 'r') as f:
                conn.executescript(f.read())

    # Read and execute views
    view_files = [
        'database/views/vw_sales_summary.sql',
        'database/views/vw_product_performance.sql'
    ]

    for view_file in view_files:
        if os.path.exists(view_file):
            with open(view_file, 'r') as f:
                conn.executescript(f.read())

    conn.commit()
    conn.close()
    logger.info("Database setup completed")

def generate_sample_data():
    """Generate sample data for demonstration"""
    logger.info("Generating sample data...")
    fake = Faker()

    # Generate products
    products = []
    categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports']
    for i in range(1, 51):
        products.append({
            'product_id': i,
            'product_name': fake.word().capitalize() + ' ' + fake.word().capitalize(),
            'category': random.choice(categories),
            'price': round(random.uniform(10, 1000), 2)
        })

    # Generate customers
    customers = []
    for i in range(1, 101):
        customers.append({
            'customer_id': i,
            'customer_name': fake.name(),
            'email': fake.email(),
            'region': random.choice(['North', 'South', 'East', 'West'])
        })

    # Generate sales
    sales = []
    for i in range(1, 1001):
        sales.append({
            'sale_id': i,
            'product_id': random.randint(1, 50),
            'customer_id': random.randint(1, 100),
            'quantity': random.randint(1, 10),
            'sale_date': fake.date_between(start_date='-1y', end_date='today'),
            'amount': round(random.uniform(10, 5000), 2)
        })

    # Save to CSV files
    pd.DataFrame(products).to_csv('data/products.csv', index=False)
    pd.DataFrame(customers).to_csv('data/customers.csv', index=False)
    pd.DataFrame(sales).to_csv('data/sales.csv', index=False)

    logger.info("Sample data generated")

def display_data():
    """Display sample data from the database in table format"""
    logger.info("Displaying sample data from database...")

    conn = sqlite3.connect('sales_analytics.db')

    # Helper function to print a table
    def print_table(title, headers, rows):
        print(f"\n{title}")
        print("-" * len(title))

        if not rows:
            print("No data available")
            return

        # Calculate column widths
        col_widths = []
        for i in range(len(headers)):
            max_width = len(str(headers[i]))
            for row in rows:
                if i < len(row):  # Check if column exists
                    max_width = max(max_width, len(str(row[i])))
            col_widths.append(max_width + 2)

        # Print header
        header_line = "|"
        for i, header in enumerate(headers):
            header_line += f" {header:<{col_widths[i]-1}}|"
        print(header_line)

        # Print separator
        separator = "|"
        for width in col_widths:
            separator += "-" * width + "|"
        print(separator)

        # Print rows
        for row in rows:
            row_line = "|"
            for i, header in enumerate(headers):
                if i < len(row):  # Check if column exists
                    value = row[i]
                else:
                    value = "N/A"
                row_line += f" {str(value):<{col_widths[i]-1}}|"
            print(row_line)

        print("")  # Empty line after table

    # Display Products (sample of 10)
    cursor = conn.cursor()
    cursor.execute("SELECT product_id, product_name, category, price FROM products LIMIT 10")
    products = cursor.fetchall()
    print_table("PRODUCTS (First 10)", 
                ["Product ID", "Product Name", "Category", "Price"],
                products)

    # Display Customers (sample of 10)
    cursor.execute("SELECT customer_id, customer_name, email, region FROM customers LIMIT 10")
    customers = cursor.fetchall()
    print_table("CUSTOMERS (First 10)", 
                ["Customer ID", "Customer Name", "Email", "Region"],
                customers)

    # Display Sales (sample of 10)
    cursor.execute("SELECT sale_id, product_id, customer_id, quantity, amount, sale_date FROM sales LIMIT 10")
    sales = cursor.fetchall()
    print_table("SALES (First 10)", 
                ["Sale ID", "Product ID", "Customer ID", "Quantity", "Amount", "Sale Date"],
                sales)

    # Display Sales Summary View
    cursor.execute("""
        SELECT strftime('%Y-%m', s.sale_date) as month, 
               p.category, 
               COUNT(*) as transactions,
               SUM(s.amount) as total_revenue
        FROM sales s
        JOIN products p ON s.product_id = p.product_id
        GROUP BY strftime('%Y-%m', s.sale_date), p.category
        ORDER BY month DESC, total_revenue DESC
        LIMIT 10
    """)
    summary = cursor.fetchall()
    print_table("SALES SUMMARY BY MONTH AND CATEGORY (First 10)", 
                ["Month", "Category", "Transactions", "Total Revenue"],
                summary)

    conn.close()

def main():
    """Main execution function"""
    try:
        # Create necessary directories
        os.makedirs('data', exist_ok=True)
        os.makedirs('output', exist_ok=True)

        # Setup database
        setup_database()

        # Generate sample data
        generate_sample_data()

        # Initialize and run pipeline
        pipeline = SalesDataPipeline('config/pipeline_config.json')
        pipeline.run()

        logger.info("Pipeline execution completed successfully")

        # Display data in console
        display_data()

    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()