# azure-etl-pipeline-demo
# Sales Analytics Data Pipeline

A demonstration of modern data engineering practices using Python, SQL, and ETL/ELT patterns. This project showcases a metadata-driven data pipeline that simulates Azure Data Factory patterns while using SQLite as the database backend.

## 🚀 Project Overview

This project implements a complete ETL (Extract, Transform, Load) pipeline for sales analytics data. It demonstrates:

- Metadata-driven pipeline architecture
- Data quality framework
- Star schema data modeling
- SQL database operations
- Automated data transformations
- Logging and error handling

  **REPLIT LINK**
  https://replit.com/join/kbaiqiuxfv-cwalding1300

## 💡 What This Project Does

1. **Generates Sample Data**: Creates realistic sales, product, and customer data
2. **Extracts Data**: Reads data from CSV files
3. **Transforms Data**: Cleans, standardizes, and enriches the data
4. **Validates Data**: Runs quality checks to ensure data integrity
5. **Loads Data**: Stores processed data in a SQLite database
6. **Creates Analytics Views**: Builds views for business intelligence

## 🛠️ Technologies Used

- **Python 3.8+**: Core programming language
- **SQLite**: Database engine
- **Pandas**: Data manipulation and analysis
- **Loguru**: Advanced logging
- **Faker**: Test data generation
- **pytest**: Testing framework

## 📁 Project Structure

```
sales-analytics-pipeline/
├── README.md                    # Project documentation
├── requirements.txt             # Python dependencies  
├── main.py                      # Main entry point
├── database/
│   ├── schema.sql              # Database schema design
│   ├── stored_procedures/      # SQL procedures (simulated as views)
│   └── views/                  # Analytical views
├── etl/
│   ├── pipeline.py             # Core pipeline logic
│   ├── transformations.py      # Data transformation functions
│   └── quality_checks.py       # Data quality framework
├── config/
│   ├── pipeline_config.json    # Pipeline configuration
│   └── logging_config.py       # Logging setup
├── tests/                      # Unit and integration tests
└── docs/                       # Additional documentation
```

## 🚀 Getting Started

### Prerequisites

- Python 3.8 or higher
- pip (Python package manager)

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/sales-analytics-pipeline.git
   cd sales-analytics-pipeline
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Run the pipeline:
   ```bash
   python main.py
   ```

## 📊 Output

The pipeline will:
1. Create a SQLite database with sales analytics data
2. Display sample data in console tables
3. Generate logs showing the pipeline execution
4. Create analytical views for reporting

Example console output:
```
PRODUCTS (First 10)
-------------------
| Product ID | Product Name | Category | Price |
|------------|-------------|----------|-------|
| 1          | Widget A    | Electronics | 299.99 |
| 2          | Gadget B    | Home       | 149.99 |
...
```

## 🔍 Key Features

### 1. Metadata-Driven Pipeline
The pipeline is configured using JSON, making it easy to adapt to different data sources and destinations.

### 2. Data Quality Framework
Automated checks for:
- Null values
- Data ranges
- Duplicates
- Custom business rules

### 3. Star Schema Design
Implements a data warehouse pattern with:
- Fact tables (sales transactions)
- Dimension tables (products, customers, dates)
- Slowly Changing Dimensions (SCD Type 2)

### 4. Error Handling & Logging
Comprehensive error handling with detailed logging for monitoring and debugging.

## 🔄 Pipeline Architecture

The pipeline follows a medallion architecture:
1. **Bronze Layer**: Raw data ingestion
2. **Silver Layer**: Cleaned and transformed data
3. **Gold Layer**: Business-ready analytics tables

## 🧪 Testing

Run the test suite:
```bash
pytest tests/
```

## 🔧 Configuration

Edit `config/pipeline_config.json` to customize:
- Data sources
- Transformation rules
- Quality check thresholds
- Logging preferences

## 📈 Future Enhancements

- Integration with Azure Data Factory
- Real-time data processing
- Advanced analytics and ML integration
- API endpoints for data access
- Containerization with Docker

## 👨‍💻 Author

Connor Walding
