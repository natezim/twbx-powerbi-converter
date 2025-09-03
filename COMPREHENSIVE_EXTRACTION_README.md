# Comprehensive Tableau to Power BI Converter

## 🚀 NEW: Comprehensive Data Extraction

This enhanced version of the Tableau to Power BI Converter now includes **comprehensive data extraction** capabilities that extract **ALL available data** from Tableau workbooks using the full power of the Tableau Document API.

## 📊 What's New

### Comprehensive JSON Extraction
The new comprehensive extraction system extracts **everything** from your Tableau workbooks:

- **Workbook Metadata**: Name, version, author, complexity analysis
- **Data Sources**: Complete connection details, custom SQL, relationships
- **Fields**: All fields with datatypes, roles, calculations, parameters
- **Worksheets**: Visual structure, encoding, filters, formatting
- **Dashboards**: Layout, contained objects, actions, formatting
- **Calculated Fields**: Formulas, dependencies, complexity analysis
- **Parameters**: Types, values, allowed ranges
- **Power BI Migration Guide**: Step-by-step migration instructions

### Enhanced GUI
The GUI now includes a new **"🚀 Comprehensive Extract"** button that:
- Extracts ALL available data in one operation
- Generates detailed Power BI migration reports
- Creates individual JSON files for each component
- Maintains backward compatibility with existing CSV/text exports

## 🔧 Usage

### Command Line
```bash
# Run comprehensive extraction on all Tableau files
python comprehensive_main.py

# Run the enhanced GUI
python gui.py
```

### GUI Usage
1. **Select Tableau File**: Browse and select your .twb or .twbx file
2. **Choose Extraction Type**:
   - **🔍 Analyze Tableau**: Quick analysis (existing functionality)
   - **🚀 Comprehensive Extract**: Extract ALL data (NEW!)
   - **📊 Extract Hyper Data**: Extract embedded data (existing functionality)

## 📁 Output Files

The comprehensive extraction generates multiple output files:

### Main Files
- `{workbook}_comprehensive.json` - Complete extracted data
- `{workbook}_migration_report.json` - Detailed Power BI migration guide

### Component Files
- `{workbook}_workbook_metadata.json` - Workbook information
- `{workbook}_datasources.json` - Data source details
- `{workbook}_fields_comprehensive.json` - All fields and calculations
- `{workbook}_worksheets.json` - Worksheet visual structure
- `{workbook}_dashboards.json` - Dashboard layout and objects
- `{workbook}_parameters.json` - Parameter definitions
- `{workbook}_calculated_fields.json` - Calculated field details
- `{workbook}_powerbi_migration_guide.json` - Migration instructions

### Legacy Files (for compatibility)
- Setup guides (`.txt`)
- Field mapping CSV
- Dashboard usage CSV
- Thumbnail screenshots

## 📋 JSON Structure

The comprehensive JSON contains the complete structure:

```json
{
  "extraction_metadata": {
    "extraction_timestamp": "2024-01-01T12:00:00",
    "extractor_version": "1.0.0",
    "tableau_document_api_version": "2023.1+",
    "extraction_scope": "comprehensive_all_data",
    "powerbi_compatibility": "2023+"
  },
  "workbook_metadata": {
    "name": "Sales Dashboard",
    "version": "2023.1",
    "author": "User",
    "total_sheets": 15,
    "complexity_score": "Medium",
    "total_fields": 45,
    "total_calculations": 8,
    "total_parameters": 3
  },
  "datasources": [
    {
      "name": "Sales Data",
      "connections": [
        {
          "dbclass": "sqlserver",
          "server": "server.company.com",
          "dbname": "SalesDB",
          "connection_string": "Type: sqlserver | Server: server.company.com | Database: SalesDB"
        }
      ],
      "tables": [
        {"name": "Orders", "type": "table"},
        {"name": "Customers", "type": "table"}
      ],
      "custom_sql": [
        {
          "name": "Custom Query",
          "sql": "SELECT * FROM Orders WHERE Date > '2023-01-01'",
          "type": "custom_sql"
        }
      ],
      "field_count": 25
    }
  ],
  "fields_comprehensive": {
    "regular_fields": [
      {
        "name": "Order Date",
        "caption": "Order Date",
        "datatype": "date",
        "role": "dimension",
        "powerbi_equivalent": "Date (Dimension)"
      }
    ],
    "calculated_fields": [
      {
        "name": "Profit Margin",
        "formula": "[Profit] / [Sales]",
        "dependencies": ["Profit", "Sales"],
        "complexity": "Simple",
        "powerbi_equivalent": "Profit Margin = DIVIDE([Profit], [Sales])",
        "used_in_sheets": ["Sheet1", "Dashboard1"]
      }
    ],
    "parameters": [
      {
        "name": "Date Range",
        "type": "Date",
        "current_value": "2023-01-01",
        "allowed_values": {"type": "Range"},
        "powerbi_steps": [
          "Create Date parameter",
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
        "x_axis": {"field": "Date"},
        "y_axis": {"field": "Sales"},
        "color": {"field": "Region"}
      },
      "filters": [
        {
          "name": "Region Filter",
          "type": "categorical",
          "field": "Region",
          "values": ["North", "South", "East", "West"]
        }
      ],
      "powerbi_instructions": [
        "Create Line Chart visual",
        "Add Date to X-axis",
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
          "Set page size to match dashboard dimensions"
        ]
      }
    }
  ],
  "powerbi_migration_guide": {
    "overview": "Comprehensive migration guide from Tableau to Power BI",
    "steps": [
      "1. Set up Power BI workspace and data sources",
      "2. Import data using Power Query",
      "3. Create calculated columns and measures",
      "4. Build visuals matching Tableau worksheets",
      "5. Create dashboard pages",
      "6. Configure filters and interactions",
      "7. Test and validate results"
    ],
    "function_mapping": {
      "Tableau": "Power BI DAX",
      "SUM": "SUM",
      "AVG": "AVERAGE",
      "COUNT": "COUNT",
      "IF": "IF"
    },
    "best_practices": [
      "Use Power Query for data transformation",
      "Create measures for calculated fields",
      "Use proper data modeling",
      "Optimize for performance"
    ]
  }
}
```

## 🎯 Key Features

### 1. Complete Data Extraction
- **Everything in the XML**: As stated in Tableau documentation, "You can literally extract everything out of the XML, from your data source connection information to field aliases and calculations. Basically everything you would drag and drop, colour or alias in Tableau is documented in the XML"
- **Document API Integration**: Uses the official Tableau Document API for reliable data access
- **Comprehensive Coverage**: Extracts workbook, datasource, worksheet, dashboard, field, and calculation data

### 2. Power BI Migration Support
- **Migration Reports**: Detailed step-by-step migration guides
- **DAX Conversion**: Automatic conversion of Tableau formulas to DAX
- **Visual Mapping**: Maps Tableau chart types to Power BI visuals
- **Complexity Assessment**: Analyzes migration difficulty and estimates time

### 3. Enhanced Analysis
- **Complexity Scoring**: Automatically assesses workbook complexity (Low/Medium/High)
- **Field Usage Tracking**: Identifies which fields are used in which worksheets
- **Dependency Analysis**: Maps calculated field dependencies
- **Connection Analysis**: Extracts detailed connection information

### 4. Multiple Output Formats
- **JSON**: Machine-readable comprehensive data
- **CSV**: Human-readable field mappings (legacy compatibility)
- **Text**: Setup guides and instructions (legacy compatibility)
- **Images**: Thumbnail screenshots of worksheets

## 🔧 Technical Details

### Architecture
- **ComprehensiveTableauExtractor**: Core extraction engine
- **EnhancedTableauMigrator**: Orchestrates comprehensive extraction
- **Backward Compatibility**: Maintains existing functionality

### Dependencies
- **Tableau Document API**: For reliable workbook parsing
- **XML Processing**: For detailed metadata extraction
- **JSON Export**: For structured data output

### Performance
- **Efficient Processing**: Optimized for large workbooks
- **Memory Management**: Handles large datasets efficiently
- **Progress Tracking**: Real-time progress updates in GUI

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

## 🚀 Getting Started

1. **Install Dependencies**: Ensure all required packages are installed
2. **Run Comprehensive Extraction**: Use the new comprehensive extraction feature
3. **Review Output**: Examine the generated JSON files and migration reports
4. **Plan Migration**: Use the migration guide to plan your Power BI implementation
5. **Execute Migration**: Follow the step-by-step instructions

## 📞 Support

The comprehensive extraction system is designed to be robust and handle various Tableau workbook configurations. If you encounter issues:

1. Check the log output for detailed error messages
2. Verify your Tableau file is not corrupted
3. Ensure you have the latest version of the Tableau Document API
4. Review the generated JSON files for data completeness

## 🔄 Version History

- **v1.0.0**: Initial comprehensive extraction implementation
- **Enhanced GUI**: Added comprehensive extraction button
- **Migration Reports**: Added detailed Power BI migration guides
- **JSON Export**: Added structured JSON output
- **Backward Compatibility**: Maintained existing CSV/text exports

---

**Ready to extract everything from your Tableau workbooks? Use the new comprehensive extraction feature to get complete visibility into your data and streamline your Power BI migration!** 🚀
