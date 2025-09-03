# Tableau to Power BI Converter

A comprehensive Python tool that extracts **ALL available data** from Tableau workbooks (.twb/.twbx) and generates complete Power BI migration guides with detailed JSON output, SQL-ready column lists, and relationship mappings.

## 🚀 Key Features

### Comprehensive Data Extraction
- **Complete Workbook Analysis**: Extracts everything from Tableau workbooks using the official Tableau Document API
- **Single JSON Output**: All data consolidated into one comprehensive JSON file
- **Enhanced GUI**: Streamlined interface with one-click analysis and extraction
- **Windows File Metadata**: Extracts actual file owner and modification dates
- **Thumbnail Screenshots**: Automatic extraction of worksheet thumbnails

### Data Coverage
- **Workbook Metadata**: Name, version, author, complexity analysis, file timestamps
- **Data Sources**: Complete connection details, custom SQL, relationships, BigQuery support
- **Fields**: All fields with datatypes, roles, calculations, parameters, and usage tracking
- **Worksheets**: Visual structure, encoding, filters, formatting, field placements
- **Dashboards**: Layout, contained objects, actions, device layouts
- **Calculated Fields**: Formulas, dependencies, complexity analysis, DAX equivalents
- **Parameters**: Types, values, allowed ranges, Power BI setup steps

### Power BI Migration Support
- **Migration Reports**: Detailed step-by-step migration guides
- **DAX Conversion**: Automatic conversion of Tableau formulas to DAX
- **Visual Mapping**: Maps Tableau chart types to Power BI visuals
- **Complexity Assessment**: Analyzes migration difficulty and estimates time
- **Connection Setup**: Complete BigQuery, SQL Server, PostgreSQL, and other database setup instructions

## 🎯 Supported Database Types

- **Cloud Databases**: BigQuery, Snowflake, Redshift, Azure SQL
- **Traditional Databases**: PostgreSQL, MySQL, SQL Server, Oracle
- **File Sources**: Excel, CSV, Hyper extracts
- **Cloud Platforms**: AWS, Azure, GCP with proper authentication

## 📦 Installation

1. **Clone the repository**:
```bash
git clone https://github.com/natezim/twbx-powerbi-converter.git
cd twbx-powerbi-converter
```

2. **Install dependencies**:
```bash
pip install -r requirements.txt
```

## 🚀 Usage

### GUI Mode (Recommended)
```bash
python gui.py
```

**Simple Workflow**:
1. **Browse**: Select your Tableau file (.twb or .twbx)
2. **Analyze**: Click "🚀 Analyze & Extract All Data"
3. **Review**: View comprehensive results in the GUI
4. **Export**: Get complete JSON file with all workbook data

### Command Line Mode
```bash
python main.py
```

## 📁 Output Files

### Primary Output
- **`{workbook}_complete.json`** - Single comprehensive file with ALL extracted data
- **`screenshots/`** - Thumbnail images of all worksheets

### JSON Structure
```json
{
  "workbook_info": {
    "name": "Sales Dashboard",
    "original_filename": "Sales_Dashboard.twbx",
    "extraction_timestamp": "2024-01-01T12:00:00",
    "total_datasources": 3,
    "total_worksheets": 5,
    "total_dashboards": 2,
    "complexity_score": "Medium"
  },
  "workbook_metadata": {
    "name": "Sales Dashboard",
    "version": "2023.1",
    "author": "DOMAIN\\username",
    "creation_date": "2024-01-01T10:00:00",
    "modified_date": "2024-01-01T11:30:00",
    "total_fields": 45,
    "total_calculations": 8,
    "total_parameters": 3
  },
  "datasources": [
    {
      "name": "Sales Data",
      "caption": "Sales Database",
      "connections": [
        {
          "class": "bigquery",
          "CATALOG": "publicdata",
          "EXECCATALOG": "billing-project-123",
          "project": "publicdata",
          "username": "user@company.com",
          "authentication": "oauth"
        }
      ],
      "tables": [
        {
          "name": "[publicdata.samples].[orders]",
          "full_reference": "billing-project-123.samples.orders"
        }
      ],
      "custom_sql": [
        {
          "name": "Custom Query",
          "sql": "SELECT * FROM `publicdata.samples.orders` WHERE date > '2023-01-01'",
          "connection": "BigQuery: billing-project-123.samples"
        }
      ],
      "field_count": 25
    }
  ],
  "fields_comprehensive": {
    "regular_fields": [
      {
        "name": "order_date",
        "caption": "Order Date",
        "datatype": "date",
        "role": "dimension",
        "table_reference": "orders",
        "used_in_worksheets": ["Sales Trend", "Revenue Table"]
      }
    ],
    "calculated_fields": [
      {
        "name": "profit_margin",
        "caption": "Profit Margin",
        "formula": "[Profit] / [Sales]",
        "dependencies": ["Profit", "Sales"],
        "complexity": "Simple",
        "powerbi_equivalent": "Profit Margin = DIVIDE([Profit], [Sales])",
        "used_in_worksheets": ["Sales Trend"]
      }
    ],
    "parameters": [
      {
        "name": "date_range",
        "caption": "Date Range",
        "type": "date",
        "current_value": "2023-01-01",
        "powerbi_steps": [
          "Create Date parameter in Power BI",
          "Set default value to 2023-01-01"
        ]
      }
    ]
  },
  "worksheets": [
    {
      "name": "Sales Trend",
      "chart_type": "Line Chart",
      "encoding": {
        "x_axis": {"field": "Order Date", "type": "temporal"},
        "y_axis": {"field": "Sales", "aggregation": "SUM"},
        "color": {"field": "Region", "type": "nominal"}
      },
      "filters": [
        {
          "name": "Region Filter",
          "field": "Region",
          "type": "categorical",
          "values": ["North", "South", "East", "West"]
        }
      ],
      "powerbi_instructions": [
        "Create Line Chart visual in Power BI",
        "Add Order Date to X-axis",
        "Add SUM(Sales) to Y-axis",
        "Add Region to Legend"
      ]
    }
  ],
  "dashboards": [
    {
      "name": "Executive Dashboard",
      "size": {"width": "1200", "height": "800"},
      "layout_type": "Tiled",
      "contained_objects": [
        {"type": "worksheet", "name": "Sales Trend"},
        {"type": "worksheet", "name": "Revenue Table"}
      ],
      "powerbi_migration": {
        "report_pages": 1,
        "suggested_layout": "Grid",
        "instructions": [
          "Create new Power BI report page",
          "Set page size to 1200x800 pixels"
        ]
      }
    }
  ],
  "powerbi_migration_guide": {
    "overview": "Complete migration guide from Tableau to Power BI",
    "steps": [
      "1. Set up Power BI workspace and data sources",
      "2. Import data using Power Query",
      "3. Create calculated columns and measures",
      "4. Build visuals matching Tableau worksheets",
      "5. Create dashboard pages",
      "6. Configure filters and interactions"
    ],
    "function_mapping": {
      "Tableau": "Power BI DAX",
      "SUM": "SUM",
      "AVG": "AVERAGE",
      "COUNT": "COUNT",
      "IF": "IF"
    }
  }
}
```

## 🏗️ Project Structure

```
twbx-powerbi-converter/
├── gui.py                           # Main GUI application
├── main.py                          # Command line entry point
├── core/
│   ├── comprehensive_extractor.py   # Core comprehensive extraction engine
│   ├── enhanced_migrator.py         # Orchestrates extraction and export
│   ├── tableau_parser.py            # TWBX/TWB file parsing
│   ├── field_extractor.py           # Field metadata extraction
│   ├── sql_generator.py             # SQL and relationship extraction
│   ├── csv_exporter.py              # Legacy output generation
│   ├── thumbnail_extractor.py       # Screenshot extraction
│   └── tableaudocumentapi/          # Official Tableau Document API
├── utils/
│   └── file_utils.py                # File handling utilities
├── output/                          # Generated files (gitignored)
├── requirements.txt                 # Python dependencies
└── README.md                        # This file
```

## 🔧 Technical Details

### Architecture
- **ComprehensiveTableauExtractor**: Core extraction engine using Tableau Document API
- **EnhancedTableauMigrator**: Orchestrates comprehensive extraction and JSON export
- **Streamlined GUI**: Single-button interface for complete analysis
- **Backward Compatibility**: Maintains existing functionality

### Key Technologies
- **Tableau Document API**: Official API for reliable workbook parsing
- **XML Processing**: Detailed metadata extraction from workbook XML
- **JSON Export**: Structured data output for easy consumption
- **Tkinter GUI**: Cross-platform graphical interface
- **Threading**: Non-blocking analysis for large workbooks

### Performance Features
- **Efficient Processing**: Optimized for large workbooks with many worksheets
- **Memory Management**: Handles large datasets efficiently
- **Progress Tracking**: Real-time progress updates in GUI
- **Error Handling**: Robust error handling with detailed logging

## 📈 Migration Benefits

### For Data Analysts
- **Complete Visibility**: See everything in your Tableau workbook
- **Migration Planning**: Understand complexity before starting migration
- **Field Mapping**: Clear mapping of all fields and calculations
- **Visual Recreation**: Step-by-step instructions for recreating visuals

### For IT Teams
- **Data Source Analysis**: Complete connection and SQL information
- **Security Assessment**: Understand data access patterns
- **Performance Planning**: Assess migration complexity and time
- **Documentation**: Comprehensive documentation of existing workbooks

### For Organizations
- **Migration Strategy**: Plan large-scale Tableau to Power BI migrations
- **Knowledge Transfer**: Preserve institutional knowledge
- **Compliance**: Maintain audit trails of data transformations
- **Training**: Use extracted data for team training

## 🎯 Example Use Cases

### BigQuery Migration
```json
{
  "datasources": [{
    "connections": [{
      "class": "bigquery",
      "CATALOG": "publicdata",
      "EXECCATALOG": "billing-project-123",
      "project": "publicdata"
    }],
    "custom_sql": [{
      "sql": "SELECT * FROM `publicdata.samples.orders` WHERE date > '2023-01-01'"
    }]
  }]
}
```

### Complex Calculated Fields
```json
{
  "calculated_fields": [{
    "name": "profit_margin",
    "formula": "([Revenue] - [Cost]) / [Revenue]",
    "powerbi_equivalent": "Profit Margin = DIVIDE([Revenue] - [Cost], [Revenue])",
    "complexity": "Medium"
  }]
}
```

## 📋 Requirements

- **Python 3.7+**
- **Core Dependencies**:
  - `pandas` - Data manipulation
  - `lxml` - XML processing
  - `openpyxl` - Excel file support
  - `tkinter` - GUI framework (included with Python)

## 🚀 Getting Started

1. **Install Dependencies**: `pip install -r requirements.txt`
2. **Run GUI**: `python gui.py`
3. **Select File**: Browse and select your Tableau workbook
4. **Analyze**: Click "🚀 Analyze & Extract All Data"
5. **Review Results**: Check the comprehensive JSON output
6. **Plan Migration**: Use the migration guide for Power BI setup

## 📞 Support

The comprehensive extraction system is designed to be robust and handle various Tableau workbook configurations. If you encounter issues:

1. Check the GUI log output for detailed error messages
2. Verify your Tableau file is not corrupted
3. Ensure you have the latest version of dependencies
4. Review the generated JSON files for data completeness

## 📄 License

MIT License - see LICENSE file for details.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with various Tableau workbooks
5. Submit a pull request

---

**Ready to extract everything from your Tableau workbooks? Use the comprehensive extraction feature to get complete visibility into your data and streamline your Power BI migration!** 🚀