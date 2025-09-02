#!/usr/bin/env python3
"""
Tableau to Power BI Converter - Master Orchestrator
Coordinates all modules for TWBX processing and migration
"""

import os
from core.tableau_parser import TableauParser
from core.field_extractor import FieldExtractor
from core.sql_generator import SQLGenerator
from core.csv_exporter import CSVExporter
from utils.file_utils import find_twbx_files, create_safe_filename, ensure_directory_exists, validate_twbx_file


class TableauMigrator:
    """Master orchestrator for Tableau to Power BI conversion."""
    
    def __init__(self):
        self.parser = None
        self.field_extractor = None
        self.sql_generator = None
        self.csv_exporter = CSVExporter()
    
    def process_twbx_file(self, twbx_path, skip_hyper_extraction=False):
        """Process a single TWBX file through the complete pipeline."""
        print(f"🔄 Processing: {twbx_path}")
        print("-" * 50)
        
        try:
            # 1. Parse TWBX file
            self.parser = TableauParser(twbx_path)
            if not self.parser.extract_and_parse():
                print(f"❌ Failed to parse {twbx_path}")
                return None
            
            print("✅ TWBX parsed successfully using official Tableau API + XML")
            
            # 2. Extract data sources
            print("🔍 Analyzing data sources...")
            data_sources = self.extract_data_sources(skip_hyper_extraction)
            
            if not data_sources:
                print("❌ No data sources found")
                return None
            
            # 3. Display data source summary
            for ds in data_sources:
                print(f"✅ Found: {ds['caption']} ({ds['name']})")
                print(f"   Connections: {len(ds['connections'])}")
                print(f"   Fields: {ds['field_count']}")
                
                for conn in ds['connections']:
                    if conn['dbclass']:
                        print(f"     - {conn['dbclass']}: {conn['server']}/{conn['dbname']}")
                    elif conn['filename']:
                        print(f"     - File: {conn['filename']}")
                print()
            
            return data_sources
            
        except Exception as e:
            print(f"❌ Error processing {twbx_path}: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def extract_data_sources(self, skip_hyper_extraction=False):
        """Extract data sources using the modular approach."""
        if not self.parser:
            return []
        
        workbook = self.parser.get_workbook()
        xml_root = self.parser.get_xml_root()
        
        if not workbook or not xml_root:
            return []
        
        # Get actual TWBX filename for file naming
        twbx_filename = os.path.basename(self.parser.twbx_path)
        workbook_name = os.path.splitext(twbx_filename)[0]  # Remove .twbx extension
        
        # Initialize extractors
        self.field_extractor = FieldExtractor(xml_root, workbook)
        self.xml_root = xml_root
        self.sql_generator = SQLGenerator(xml_root)
        
        data_sources = []
        
        # Add workbook name to each data source for proper file naming
        for datasource in workbook.datasources:
            ds_info = {
                'name': datasource.name,
                'caption': datasource.caption,
                'workbook_name': workbook_name,  # Add workbook name for file naming
                'twbx_path': self.parser.twbx_path,  # Add TWBX file path for copying
                'connections': [],
                'fields': [],
                'field_count': 0
            }
            
            # Special handling for Parameters datasource - treat as calculated fields
            if datasource.name == 'Parameters':
                print(f"   Found Parameters datasource - will merge with main datasource")
                continue  # Skip creating separate datasource entry for Parameters
            
            # Get connections using official API
            for connection in datasource.connections:
                conn_info = {
                    'server': getattr(connection, 'server', ''),
                    'dbname': getattr(connection, 'dbname', ''),
                    'username': getattr(connection, 'username', ''),
                    'dbclass': getattr(connection, 'dbclass', ''),
                    'port': getattr(connection, 'port', ''),
                    'schema': getattr(connection, 'schema', ''),
                    'filename': getattr(connection, 'filename', '')
                }
                ds_info['connections'].append(conn_info)
            
            # Get rich field metadata from XML
            xml_metadata = self.field_extractor.extract_field_metadata(datasource.name)
            print(f"   Found {len(xml_metadata)} fields with rich metadata from XML")
            
            # Extract calculated fields from workbook XML
            self.field_extractor.extract_calculated_fields_from_workbook(xml_metadata)
            
            # Extract data from Hyper files (if any exist and not skipped)
            if skip_hyper_extraction:
                print("🔍 Skipping Hyper data extraction (analysis mode)")
                hyper_data = {}
            else:
                print("🔍 Looking for Hyper data files...")
                hyper_data = self.field_extractor.extract_data_from_hyper_files(self.parser.twbx_path)
                
                # Check if Hyper API dependency is missing
                if hyper_data.get("__missing_dependency__"):
                    print(f"   ⚠️ Hyper data extraction skipped: {hyper_data['__missing_dependency__']} not available")
                    print("   Install with: pip install tableauhyperapi")
                    hyper_data = {}  # Clear the marker
            
            # Get dashboard and worksheet information
            dashboard_info = self.field_extractor.extract_dashboard_worksheet_info(self.xml_root)
            print(f"   Found {len(dashboard_info)} dashboards/worksheets")
            
            # Get SQL queries from XML
            datasource_xml = self.parser.find_datasource_xml(datasource.name)
            if datasource_xml:
                sql_info = self.sql_generator.extract_sql_from_tableau_xml(datasource_xml)
                print(f"   Found {len(sql_info['custom_sql'])} custom SQL queries")
                print(f"   Found {len(sql_info['table_references'])} table references")
                print(f"   Found {len(sql_info['relationships'])} relationships")
            else:
                sql_info = {'custom_sql': [], 'table_references': [], 'relationships': []}
            
            # Create comprehensive field information
            for field_name, metadata in xml_metadata.items():
                field_info = {
                    'name': field_name,
                    'caption': field_name,
                    'datatype': metadata.get('data_type', 'Unknown'),
                    'role': metadata.get('role', 'None'),
                    'id': field_name,
                    'table_name': metadata.get('table_name', 'Unknown'),
                    'remote_name': metadata.get('remote_name', field_name),
                    'table_reference': metadata.get('table_reference', field_name),
                    'used_in_workbook': metadata.get('used_in_workbook', False),
                    'is_calculated': metadata.get('is_calculated', False),
                    'is_parameter': metadata.get('is_parameter', False),  # Add parameter flag
                    'calculation_formula': metadata.get('calculation_formula', ''),
                    'calculation_class': metadata.get('calculation_class', '')
                }
                ds_info['fields'].append(field_info)
            
            # Add hyper data if available
            if hyper_data:
                ds_info['hyper_data'] = hyper_data
                print(f"   📊 Found hyper data for {len(hyper_data)} tables")
            
            ds_info['field_count'] = len(ds_info['fields'])
            ds_info['sql_info'] = sql_info
            ds_info['dashboard_info'] = dashboard_info
            data_sources.append(ds_info)
        
        return data_sources
    
    def export_results(self, data_sources, skip_hyper_data=False):
        """Export results as text setup guides and CSV field mapping."""
        # Ensure output directory exists
        ensure_directory_exists('output')
        
        # Export text setup guides
        print("💾 Exporting setup guides...")
        self.csv_exporter.export_setup_guide_txt('output', data_sources)
        
        # Export CSV field mapping
        print("📊 Exporting field mapping CSV...")
        self.csv_exporter.export_field_mapping_csv('output', data_sources)
        
        # Export dashboard usage CSV
        print("📋 Exporting dashboard usage CSV...")
        # Only export dashboard usage for the first datasource to avoid duplicates
        if data_sources and data_sources[0].get('dashboard_info'):
            self.csv_exporter.export_dashboard_usage_csv('output', data_sources, data_sources[0]['dashboard_info'])
        
        # Export hyper data to Excel (if any exists and not skipped)
        if not skip_hyper_data:
            print("📊 Exporting Hyper data to Excel...")
            for ds in data_sources:
                if ds.get('hyper_data'):
                    # Skip if Hyper API dependency is missing
                    if ds['hyper_data'].get("__missing_dependency__"):
                        print(f"   ⚠️ Skipping Hyper data export for {ds.get('caption', 'Unknown')}: {ds['hyper_data']['__missing_dependency__']} not available")
                        continue
                    
                    # Create workbook-specific folder for hyper data
                    workbook_name = ds.get('workbook_name', 'Unknown')
                    if workbook_name is None:
                        workbook_name = 'Unknown'
                    safe_workbook = str(workbook_name).replace(' ', '_').replace('/', '_')
                    safe_workbook = ''.join(c for c in safe_workbook if c.isalnum() or c in '_-')
                    hyper_output_dir = os.path.join('output', safe_workbook)
                    os.makedirs(hyper_output_dir, exist_ok=True)
                    
                    # Export hyper data to Excel
                    self.field_extractor.export_hyper_data_to_excel(ds['hyper_data'], hyper_output_dir)
        else:
            print("📊 Skipping Hyper data export (regular analysis mode)")


def main():
    """Main entry point for the Tableau to Power BI converter."""
    print("🔍 Tableau to Power BI Converter - Modular Edition")
    print("=" * 70)
    
    # Find all TWBX files in current directory
    twbx_files = find_twbx_files('.')
    
    if not twbx_files:
        print("❌ No .twbx files found in current directory")
        print("Please place one or more .twbx files in the current directory")
        return
    
    print(f"📂 Found {len(twbx_files)} TWBX file(s): {', '.join(twbx_files)}")
    print()
    
    # Initialize the migrator
    migrator = TableauMigrator()
    
    # Process each TWBX file
    for twbx_file in twbx_files:
        # Validate the file
        is_valid, message = validate_twbx_file(twbx_file)
        if not is_valid:
            print(f"❌ Skipping {twbx_file}: {message}")
            continue
        
        # Process the file
        data_sources = migrator.process_twbx_file(twbx_file)
        
        if data_sources:
            # Export results
            migrator.export_results(data_sources)
            print(f"✅ Extraction complete for {twbx_file}!")
        else:
            print(f"❌ Failed to process {twbx_file}")
        
        print()  # Add spacing between files
    
    print("🎉 All TWBX files processed!")


if __name__ == "__main__":
    main()
