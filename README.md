# TWBX to Power BI Converter

A powerful Python tool that extracts data source information from Tableau workbooks (.twbx files) and generates comprehensive field mapping for Power BI migration.

## 🚀 Features

- **Universal Compatibility**: Works with any Tableau workbook (not just specific domains)
- **Rich Field Extraction**: Extracts field names, data types, table relationships, and connection details
- **Power BI Ready**: Generates field mapping CSV with SQL-ready table references
- **Connection Details**: Extracts database connection information (server, database, port, etc.)
- **Organized Output**: Sorts fields by table name and column name for easy navigation

## 📋 Requirements

- Python 3.7+
- `tableaudocumentapi` library
- `xml.etree.ElementTree` (built-in)
- `csv` (built-in)
- `os` (built-in)

## 🛠️ Installation

1. Clone this repository:
```bash
git clone https://github.com/yourusername/twbx-powerbi-converter.git
cd twbx-powerbi-converter
```

2. Install required dependencies:
```bash
pip install tableaudocumentapi
```

## 📖 Usage

### Basic Usage

```bash
python tableau_migrator.py
```

The tool will automatically:
1. Look for `.twbx` files in the current directory
2. Extract data source information using the official Tableau Document API
3. Parse XML metadata for rich field information
4. Generate organized CSV field mapping
5. Create SQL files with connection details and setup instructions

### Output Files

- **Field Mapping CSV**: `{datasource_name}_field_mapping.csv`
  - Original field names
  - Tableau field names
  - Data types
  - Table names
  - SQL-ready table references (quoted)

- **SQL Files**: `{datasource_name}.sql`
  - Connection details
  - Field information
  - Power BI setup instructions
  - Connection templates

## 🔍 CSV Structure

The field mapping CSV contains:

| Column | Description |
|--------|-------------|
| `Original_Field_Name` | Database field name |
| `Tableau_Field_Name` | Tableau display name |
| `Data_Type` | Field data type (string, integer, real, datetime, date) |
| `Table_Name` | Source table alias |
| `Table_Reference_SQL` | Quoted table reference for SQL queries |

## 💡 Example Output

```csv
Original_Field_Name,Tableau_Field_Name,Data_Type,Table_Name,Table_Reference_SQL
field_name,field_name (table_alias),string,table_name,'table_name.field_name'
```

## 🎯 Use Cases

- **Power BI Migration**: Map Tableau fields to Power BI data model
- **Data Documentation**: Document field relationships and data types
- **SQL Development**: Get ready-to-use table references for queries
- **Data Analysis**: Understand the structure of Tableau workbooks

## 🔧 How It Works

1. **TWBX Extraction**: Uses official Tableau Document API to handle .twbx files
2. **XML Metadata**: Extracts rich field information from Tableau's XML structure
3. **Field Mapping**: Maps original database fields to Tableau display names
4. **Table Relationships**: Identifies source tables and relationships
5. **Organized Output**: Sorts fields by table and column for easy navigation

## 📁 Project Structure

```
twbx-powerbi-converter/
├── tableau_migrator.py    # Main conversion script
├── README.md              # This file
├── requirements.txt        # Python dependencies
├── .gitignore             # Git ignore file
└── output/                # Generated files (created automatically)
    ├── *.csv              # Field mapping files
    └── *.sql              # SQL setup files
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with different .twbx files
5. Submit a pull request

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## ⚠️ Disclaimer

This tool is designed to assist with Power BI migration planning. Always verify the extracted information and test connections before implementing in production environments.

## 🆘 Support

If you encounter issues or have questions:
1. Check the output for error messages
2. Ensure your .twbx file is valid and accessible
3. Verify all dependencies are installed
4. Open an issue on GitHub with details about your problem

---

**Made with ❤️ for the Power BI community**
