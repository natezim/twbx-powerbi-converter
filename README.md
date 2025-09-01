# Tableau to Power BI Converter

A Python tool that extracts metadata from Tableau workbooks (.twbx) and generates Power BI setup guides with SQL-ready column lists and relationship mappings.

## Features

- **Universal Compatibility**: Works with any Tableau workbook regardless of database type
- **Clean Output**: Generates no-fluff setup guides for Power BI migration
- **SQL-Ready**: Provides properly formatted SQL column lists organized by table
- **Relationship Mapping**: Extracts and formats table relationships with proper JOIN syntax
- **Field Mapping**: CSV export with original vs Tableau field names
- **Calculated Field Filtering**: Automatically excludes calculated fields (database fields only)

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

1. Place your Tableau workbook (.twbx) files in the project directory
2. Run the converter:
```bash
python main.py
```

3. Check the `output/` folder for generated files:
   - `*_setup_guide.txt` - Power BI setup instructions
   - `*_field_mapping.csv` - Field mapping data

## Output Format

### Setup Guide Example
```
POWER BI SETUP GUIDE
==================

Data Source: federated.14u9jyo1x4rfq40zzuj6c1nmag30
Caption: nfl_teams (postgres)
Fields Available: 75
Fields Used in Workbook: 12

CONNECTION DETAILS:
------------------
Connection 1:
  Server: aws-0-us-east-1.pooler.supabase.com
  Database: postgres
  Username: postgres.wlmdmpgnmadyuvsiztyx
  Type: postgres
  Port: 5432

TABLES TO IMPORT:
----------------
  props_teams as Away
  nfl_teams as Away Teams
  props_teams as Home
  nfl_teams as Home Teams
  nfl_dimers_lines

MAIN TABLE: nfl_dimers_lines (aliased as nfl_dimers_lines)

CREATE THESE RELATIONSHIPS IN POWER BI MODEL VIEW:
------------------------------------------------
1. LEFT JOIN props_teams AS Home ON props_teams.abbr = nfl_teams AS "Home Teams".team_abbr
2. LEFT JOIN nfl_dimers_lines AS nfl_dimers_lines ON nfl_dimers_lines.away_team = props_teams AS Away.abbr

SQL COLUMNS:

-- Away Teams:
  "Away Teams".team_logo_squared as 'team_logo_squared (nfl_teams)',

-- Home:
  Home.full_name as full_name,

-- Home Teams:
  "Home Teams".team_division as team_division,
  "Home Teams".team_logo_squared as team_logo_squared,

-- nfl_dimers_lines:
  nfl_dimers_lines.away_team as away_team,
  nfl_dimers_lines.date as date,
  nfl_dimers_lines.game_id as game_id,
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
