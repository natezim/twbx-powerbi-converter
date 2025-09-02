# Tableau to Power BI Converter

A Python tool that extracts metadata from Tableau workbooks (.twbx) and generates Power BI setup guides with SQL-ready column lists and relationship mappings.

## Features

- **Universal Compatibility**: Works with any Tableau workbook regardless of database type
- **Clean Output**: Generates no-fluff setup guides for Power BI migration
- **SQL-Ready**: Provides properly formatted SQL column lists organized by table
- **Relationship Mapping**: Extracts and formats table relationships with proper JOIN syntax
- **Field Mapping**: CSV export with original vs Tableau field names
- **Calculated Field Filtering**: Automatically excludes calculated fields (database fields only)
- **User-Friendly GUI**: Intuitive interface with drag & drop support
- **Real-Time Progress**: Monitor conversion progress with detailed status updates
- **Comprehensive Results**: View detailed results and open output folder directly

## Supported Database Types

- PostgreSQL
- MySQL
- SQL Server
- Redshift
- BigQuery (Cloud SQL)
- Cloud Spanner
- Excel/CSV files

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd twbx-powerbi-converter
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Option 1: GUI Mode (Recommended)
1. Run the launcher:
```bash
python run.py
```
2. Choose option 1 for GUI mode
3. Use the intuitive interface to:
   - Drag & drop or browse for TWBX files
   - Monitor conversion progress
   - View results in real-time
   - Open output folder directly

### Option 2: Command Line Mode
1. Place your Tableau workbook (.twbx) files in the project directory
2. Run the converter:
```bash
python main.py
```

### Output Files
Check the `output/` folder for generated files:
   - `*_setup_guide.txt` - Power BI setup instructions
   - `*_field_mapping.csv` - Field mapping data
   - `*_dashboard_usage.csv` - Dashboard and worksheet details with filter information

## Output Format

### Setup Guide Example
```
POWER BI SETUP GUIDE
==================

Data Source: federated.sample_datasource_id
Caption: Sample Database (postgres)
Fields Available: 45
Fields Used in Workbook: 8

CONNECTION DETAILS:
------------------
Connection 1:
  Server: sample-server.database.com
  Database: sample_db
  Username: sample_user
  Type: postgres
  Port: 5432

TABLES TO IMPORT:
----------------
  customers as Customer
  orders as Order
  products as Product
  categories as Category

MAIN TABLE: orders (aliased as Order)

CREATE THESE RELATIONSHIPS IN POWER BI MODEL VIEW:
------------------------------------------------
1. LEFT JOIN customers AS Customer ON customers.customer_id = orders.customer_id
2. LEFT JOIN products AS Product ON products.product_id = orders.product_id
3. LEFT JOIN categories AS Category ON categories.category_id = products.category_id

SQL COLUMNS:

-- Customer:
  Customer.customer_name as customer_name,
  Customer.email as email,
  Customer.phone as phone,

-- Order:
  Order.order_id as order_id,
  Order.order_date as order_date,
  Order.total_amount as total_amount,

-- Product:
  Product.product_name as product_name,
  Product.price as price,

-- Category:
  Category.category_name as category_name
```

## Project Structure

```
twbx-powerbi-converter/
├── main.py                    # Entry point
├── core/
│   ├── tableau_parser.py      # TWBX file parsing
│   ├── field_extractor.py     # Field metadata extraction
│   ├── sql_generator.py       # SQL and relationship extraction
│   └── csv_exporter.py        # Output generation
├── utils/
│   └── file_utils.py          # File handling utilities
├── output/                    # Generated files (gitignored)
├── requirements.txt           # Python dependencies
└── README.md                 # This file
```

## Requirements

- Python 3.7+
- pandas
- lxml
- openpyxl

## License

MIT License - see LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with various Tableau workbooks
5. Submit a pull request

## Notes

- The tool uses the official Tableau Document API for reliable metadata extraction
- Calculated fields are automatically filtered out as they don't exist in the database
- Table aliases with spaces are properly quoted in SQL output
- The tool is designed to be universal and work with any Tableau workbook structure
