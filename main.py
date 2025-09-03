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
from core.thumbnail_extractor import ThumbnailExtractor
from utils.file_utils import find_tableau_files, find_twbx_files, create_safe_filename, ensure_directory_exists, validate_tableau_file, validate_twbx_file


class TableauMigrator:
    """Master orchestrator for Tableau to Power BI conversion."""
    
    def __init__(self):
        self.parser = None
        self.field_extractor = None
        self.sql_generator = None
        self.csv_exporter = CSVExporter()
        self.thumbnail_extractor = ThumbnailExtractor()
    
    def process_tableau_file(self, tableau_path, skip_hyper_extraction=False):
        """Process a single Tableau file (.twb or .twbx) through the complete pipeline."""
        print(f"🔄 Processing: {tableau_path}")
        print("-" * 50)
        
        try:
            # 1. Parse Tableau file
            self.parser = TableauParser(tableau_path)
            if not self.parser.extract_and_parse():
                print(f"❌ Failed to parse {tableau_path}")
                return None
            
            file_type = "TWBX" if tableau_path.endswith('.twbx') else "TWB"
            print(f"✅ {file_type} parsed successfully using official Tableau API + XML")
            
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
            print(f"❌ Error processing {tableau_path}: {e}")
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
                
                # Enhanced BigQuery and cloud database support
                # BigQuery might use different property names
                if hasattr(connection, 'project') and getattr(connection, 'project'):
                    conn_info['project'] = getattr(connection, 'project', '')
                if hasattr(connection, 'dataset') and getattr(connection, 'dataset'):
                    conn_info['dataset'] = getattr(connection, 'dataset', '')
                if hasattr(connection, 'location') and getattr(connection, 'location'):
                    conn_info['location'] = getattr(connection, 'location', '')
                if hasattr(connection, 'region') and getattr(connection, 'region'):
                    conn_info['region'] = getattr(connection, 'region', '')
                
                # For BigQuery, if Document API doesn't provide details, extract from XML
                if conn_info['dbclass'] == 'bigquery' and (not conn_info.get('project') or not conn_info['server']):
                    # Try to get BigQuery details from XML
                    xml_datasource = self.parser.find_datasource_xml(datasource.name)
                    if xml_datasource:
                        # Look for BigQuery connection in XML - check both direct and nested connections
                        bigquery_connections = (xml_datasource.findall('.//connection[@class="bigquery"]') + 
                                               xml_datasource.findall('.//named-connection//connection[@class="bigquery"]'))
                        
                        for xml_conn in bigquery_connections:
                            # Extract all BigQuery-specific attributes
                            if xml_conn.get('CATALOG'):
                                conn_info['project'] = conn_info.get('project') or xml_conn.get('CATALOG', '')
                                conn_info['server'] = conn_info.get('server') or xml_conn.get('CATALOG', '')
                            if xml_conn.get('EXECCATALOG'):
                                conn_info['billing_project'] = xml_conn.get('EXECCATALOG', '')
                            if xml_conn.get('project'):
                                conn_info['project'] = xml_conn.get('project', '')
                                conn_info['server'] = xml_conn.get('project', '')  # Use project as server for display
                            if xml_conn.get('schema'):
                                conn_info['dataset'] = xml_conn.get('schema', '')
                                conn_info['dbname'] = xml_conn.get('schema', '')  # Use schema as dbname for display
                            if xml_conn.get('authentication'):
                                conn_info['authentication'] = xml_conn.get('authentication', '')
                            if xml_conn.get('connection-dialect'):
                                conn_info['connection_dialect'] = xml_conn.get('connection-dialect', '')
                            if xml_conn.get('username'):
                                conn_info['username'] = xml_conn.get('username', '')
                            if xml_conn.get('server-oauth'):
                                conn_info['server_oauth'] = xml_conn.get('server-oauth', '')
                
                # For BigQuery, use project as the primary identifier if dbname is empty
                if conn_info['dbclass'] == 'bigquery' and not conn_info['dbname'] and conn_info.get('project'):
                    conn_info['dbname'] = conn_info['project']
                
                ds_info['connections'].append(conn_info)
            
            # Get rich field metadata from XML
            xml_metadata = self.field_extractor.extract_field_metadata(datasource.name)
            print(f"   Found {len(xml_metadata)} fields with rich metadata from XML")
            
            # ALSO extract fields using Document API for enhanced information
            print(f"   Document API fields and calculations:")
            for field_name, field_obj in datasource.fields.items():
                # Enhance XML metadata with Document API data
                if field_name in xml_metadata:
                    xml_metadata[field_name].update({
                        'api_caption': field_obj.caption,
                        'api_role': field_obj.role,  # Dimension/Measure
                        'api_type': field_obj.type,  # quantitative/ordinal/nominal
                        'api_calculation': field_obj.calculation,
                        'api_worksheets': field_obj.worksheets,
                        'api_description': field_obj.description,
                        'api_aggregation': field_obj.default_aggregation
                    })
                else:
                    # Add new field found only in Document API
                    xml_metadata[field_name] = {
                        'name': field_obj.name or field_name,
                        'caption': field_obj.caption,
                        'datatype': field_obj.datatype,
                        'role': field_obj.role,
                        'type': field_obj.type,
                        'calculation_formula': field_obj.calculation,
                        'is_calculated': field_obj.calculation is not None,
                        'worksheets': field_obj.worksheets,
                        'description': field_obj.description,
                        'default_aggregation': field_obj.default_aggregation,
                        'source': 'document_api'
                    }
                
                # Log calculated fields specifically
                if field_obj.calculation:
                    print(f"      - {field_name}: {field_obj.caption} (CALCULATED)")
                    print(f"        Formula: {field_obj.calculation[:80]}...")
                elif field_obj.role == 'measure':
                    print(f"      - {field_name}: {field_obj.caption} (MEASURE)")
                elif field_obj.role == 'dimension':
                    print(f"      - {field_name}: {field_obj.caption} (DIMENSION)")
            
            print(f"   Total enhanced fields: {len(xml_metadata)}")
            print(f"   Calculated fields via Document API: {len(datasource.calculations)}")
            
            # Extract calculated fields from workbook XML (legacy method)
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
                # Use the original method for standard SQL extraction
                sql_info = self.sql_generator.extract_sql_from_tableau_xml(datasource_xml)
                print(f"   Found {len(sql_info['custom_sql'])} custom SQL queries")
                print(f"   Found {len(sql_info['table_references'])} table references")
                print(f"   Found {len(sql_info['relationships'])} relationships")
                
                # ALSO use the new connection-type-aware extraction
                print(f"   DEBUG: Checking all connection types for datasource: {datasource.name}")
                
                # First, show connection information using Tableau Document API
                print(f"   Connections found via Document API:")
                for conn in datasource.connections:
                    print(f"      - {conn.dbclass}: {conn.server} / {conn.dbname}")
                    if hasattr(conn, 'initial_sql') and conn.initial_sql:
                        print(f"        Initial SQL: {conn.initial_sql[:50]}...")
                
                # Also check custom SQL via Document API
                if hasattr(datasource, '_get_custom_sql'):
                    custom_relations = datasource._get_custom_sql()
                    print(f"   Custom SQL relations found via Document API: {len(custom_relations)}")
                    for rel in custom_relations:
                        rel_type = rel.get('type', '')
                        rel_name = rel.get('name', 'Unknown')
                        connection = rel.get('connection', '')
                        if rel_type == 'text' and rel.text:
                            print(f"      - {rel_name} (type={rel_type}, conn={connection})")
                            print(f"        SQL: {rel.text.strip()[:50]}...")
                            # Add this to our SQL info as well (with deduplication)
                            new_sql = {
                                'name': rel_name,
                                'sql': rel.text.strip(),
                                'connection': connection,
                                'type': f'Document API Custom SQL'
                            }
                            # Check if this SQL already exists (dedupe by SQL content)
                            sql_text = new_sql.get('sql', '').strip()
                            existing_sqls = [existing.get('sql', '').strip() for existing in sql_info['custom_sql']]
                            if sql_text not in existing_sqls:
                                sql_info['custom_sql'].append(new_sql)
                
                # Use Tableau Document API for reliable SQL extraction
                print(f"   📋 Document API SQL Extraction:")
                api_sql_queries = []
                
                # Extract custom SQL using Document API
                if hasattr(datasource, '_get_custom_sql'):
                    custom_relations = datasource._get_custom_sql()
                    text_relations = [r for r in custom_relations if r.get('type') == 'text' and r.text and r.text.strip()]
                    
                    print(f"      Found {len(text_relations)} custom SQL queries via Document API")
                    for rel in text_relations:
                        rel_name = rel.get('name', 'Custom Query')
                        connection_id = rel.get('connection', '')
                        sql_text = rel.text.strip()
                        
                        # Determine database type from connection
                        db_type = 'Unknown'
                        for conn in datasource.connections:
                            if connection_id and (any(part in connection_id.lower() for part in [conn.dbclass, 'bigquery', 'postgres', 'snowflake'])):
                                db_type = conn.dbclass
                                break
                        
                        print(f"         - {rel_name} ({db_type})")
                        # Generate meaningful connection description
                        connection_desc = connection_id
                        if db_type == 'bigquery':
                            # Try to get BigQuery connection details from existing connection info
                            for conn_info in ds_info['connections']:
                                if conn_info.get('dbclass') == 'bigquery':
                                    billing_project = conn_info.get('billing_project')
                                    project = conn_info.get('project')
                                    dataset = conn_info.get('dataset')
                                    if billing_project and dataset:
                                        connection_desc = f"BigQuery: {billing_project}.{dataset}"
                                    elif project and dataset:
                                        connection_desc = f"BigQuery: {project}.{dataset}"
                                    break
                        
                        api_sql_queries.append({
                            'name': rel_name,
                            'sql': sql_text,
                            'connection': connection_desc,
                            'type': f'{db_type.title()} Custom SQL via Document API'
                        })
                
                # Extract initial SQL from connections
                for conn in datasource.connections:
                    if hasattr(conn, 'initial_sql') and conn.initial_sql and conn.initial_sql.strip():
                        print(f"         - {conn.dbclass} Initial SQL")
                        api_sql_queries.append({
                            'name': f'{conn.dbclass} Initial SQL',
                            'sql': conn.initial_sql.strip(),
                            'connection': f'{conn.server}/{conn.dbname}',
                            'type': f'{conn.dbclass.title()} Initial SQL via Document API'
                        })
                
                # Add API queries to standard SQL info structure (with deduplication)
                for sql_query in api_sql_queries:
                    # Check if this SQL already exists (dedupe by SQL content)
                    sql_text = sql_query.get('sql', '').strip()
                    existing_sqls = [existing.get('sql', '').strip() for existing in sql_info['custom_sql']]
                    if sql_text not in existing_sqls:
                        sql_info['custom_sql'].append(sql_query)
                
                if api_sql_queries:
                    print(f"   ✅ Found {len(api_sql_queries)} SQL queries via Document API")
                else:
                    print(f"   ℹ️ No custom SQL found - extracting direct table connections")
                    
                    # Extract table connections when no custom SQL exists
                    if hasattr(datasource, '_get_custom_sql'):
                        all_relations = datasource._get_custom_sql()
                        table_relations = [r for r in all_relations if r.get('type') == 'table' and r.get('table')]
                        
                        print(f"      Found {len(table_relations)} direct table connections")
                        for rel in table_relations:
                            table_name = rel.get('table', '')
                            connection_id = rel.get('connection', '')
                            
                            # Skip extract tables - these are Tableau-generated
                            if 'extract' in table_name.lower():
                                continue
                            
                            # Determine database type from connection
                            db_type = 'Unknown'
                            server_info = ''
                            for conn in datasource.connections:
                                if connection_id and (any(part in connection_id.lower() for part in [conn.dbclass, 'bigquery', 'postgres', 'snowflake'])):
                                    db_type = conn.dbclass
                                    server_info = f"{conn.server}/{conn.dbname}"
                                    break
                            
                            # Generate appropriate SQL for the table reference
                            if db_type == 'bigquery':
                                # For BigQuery, format as billing_project.dataset.table
                                # Get billing project from connection info
                                billing_project = None
                                dataset = None
                                for conn in datasource.connections:
                                    if conn.dbclass == 'bigquery':
                                        # Try to get from connection info extracted earlier
                                        for conn_info in ds_info['connections']:
                                            if conn_info.get('dbclass') == 'bigquery':
                                                billing_project = conn_info.get('billing_project') or conn_info.get('project')
                                                dataset = conn_info.get('dataset')
                                                break
                                        break
                                
                                # Format the table name properly for BigQuery
                                if billing_project and dataset:
                                    # Clean the table name - remove any existing brackets or backticks
                                    clean_table = table_name.replace('[', '').replace(']', '').replace('`', '')
                                    # Extract just the table name part if it's in format like publicdata.samples.shakespeare
                                    if '.' in clean_table:
                                        table_parts = clean_table.split('.')
                                        actual_table = table_parts[-1]  # Get the last part (table name)
                                    else:
                                        actual_table = clean_table
                                    
                                    bigquery_table = f'`{billing_project}.{dataset}.{actual_table}`'
                                    generated_sql = f'SELECT * FROM {bigquery_table};'
                                else:
                                    # Fallback to original format if we can't get billing project
                                    generated_sql = f'SELECT * FROM `{table_name}`;'
                            elif db_type in ['postgres', 'mysql']:
                                generated_sql = f'SELECT * FROM "{table_name}";'
                            elif db_type == 'sqlserver':
                                generated_sql = f'SELECT * FROM [{table_name}];'
                            else:
                                generated_sql = f'SELECT * FROM {table_name};'
                            
                            # Format the table name for display in the name field
                            if db_type == 'bigquery' and billing_project and dataset:
                                display_table_name = f'`{billing_project}.{dataset}.{actual_table}`'
                            else:
                                display_table_name = table_name
                            
                            # Generate meaningful connection description for table connections
                            connection_desc = connection_id
                            if db_type == 'bigquery':
                                # Use the same BigQuery connection details we already extracted
                                if billing_project and dataset:
                                    connection_desc = f"BigQuery: {billing_project}.{dataset}"
                                elif project and dataset:
                                    connection_desc = f"BigQuery: {project}.{dataset}"
                            
                            print(f"         - Table: {table_name} ({db_type})")
                            api_sql_queries.append({
                                'name': f'Table: {display_table_name}',
                                'sql': f'-- Direct table connection\n-- Connect to: {server_info}\n-- Table: {table_name}\n\n{generated_sql}',
                                'connection': connection_desc,
                                'type': f'{db_type.title()} Table Connection via Document API'
                            })
                    
                    # Add table connection info to SQL info structure (with deduplication)
                    for sql_query in api_sql_queries:
                        # Check if this SQL already exists (dedupe by SQL content)
                        sql_text = sql_query.get('sql', '').strip()
                        existing_sqls = [existing.get('sql', '').strip() for existing in sql_info['custom_sql']]
                        if sql_text not in existing_sqls:
                            sql_info['custom_sql'].append(sql_query)
                    
                    if api_sql_queries:
                        print(f"   ✅ Found {len(api_sql_queries)} table connections via Document API")
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
        # Use dashboard info from any datasource (they all have the same workbook-level dashboard info)
        if data_sources:
            for ds in data_sources:
                if ds.get('dashboard_info'):
                    self.csv_exporter.export_dashboard_usage_csv('output', data_sources, ds['dashboard_info'])
                    break  # Only need to export once since dashboard info is workbook-level
        
        # Extract thumbnails from the workbook
        print("🖼️  Extracting thumbnails...")
        self._extract_thumbnails(data_sources)
        
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
    
    def _extract_thumbnails(self, data_sources):
        """Extract thumbnails from the workbook and save as PNG files."""
        if not data_sources or not self.xml_root:
            print("   ℹ️  No data sources or XML root available for thumbnail extraction")
            return
        
        try:
            # Get workbook name for creating output directory
            workbook_name = data_sources[0].get('workbook_name', 'Unknown')
            if workbook_name is None:
                workbook_name = 'Unknown'
            
            # Create workbook-specific output directory
            safe_workbook = str(workbook_name).replace(' ', '_').replace('/', '_')
            safe_workbook = ''.join(c for c in safe_workbook if c.isalnum() or c in '_-')
            output_dir = os.path.join('output', safe_workbook)
            os.makedirs(output_dir, exist_ok=True)
            
            # Extract thumbnails using the thumbnail extractor
            results = self.thumbnail_extractor.extract_thumbnails(self.xml_root, output_dir)
            
            # Display summary
            if results['extracted_count'] > 0:
                print(f"   ✅ Successfully extracted {results['extracted_count']} thumbnail(s)")
                print(f"   📁 Saved to: {results['screenshots_dir']}")
                
                # Display individual files
                for file_info in results['saved_files']:
                    size_kb = file_info['file_size'] / 1024
                    print(f"      • {file_info['filename']} ({file_info['dimensions']}, {size_kb:.1f} KB)")
            else:
                print("   ℹ️  No thumbnails found in this workbook")
            
            # Display any errors
            if results['errors']:
                print("   ⚠️  Thumbnail extraction errors:")
                for error in results['errors']:
                    print(f"      • {error}")
                    
        except Exception as e:
            print(f"   ❌ Thumbnail extraction failed: {str(e)}")


def main():
    """Main entry point for the Tableau to Power BI converter."""
    print("🔍 Tableau to Power BI Converter - Modular Edition")
    print("=" * 70)
    
    # Find all Tableau files (.twb and .twbx) in current directory
    tableau_files = find_tableau_files('.')
    
    if not tableau_files:
        print("❌ No Tableau files (.twb or .twbx) found in current directory")
        print("Please place one or more Tableau files in the current directory")
        return
    
    print(f"📂 Found {len(tableau_files)} Tableau file(s): {', '.join(tableau_files)}")
    print()
    
    # Initialize the migrator
    migrator = TableauMigrator()
    
    # Process each Tableau file
    for tableau_file in tableau_files:
        # Validate the file
        is_valid, message = validate_tableau_file(tableau_file)
        if not is_valid:
            print(f"❌ Skipping {tableau_file}: {message}")
            continue
        
        # Process the file
        data_sources = migrator.process_tableau_file(tableau_file)
        
        if data_sources:
            # Export results
            migrator.export_results(data_sources)
            print(f"✅ Extraction complete for {tableau_file}!")
        else:
            print(f"❌ Failed to process {tableau_file}")
        
        print()  # Add spacing between files
    
    print("🎉 All Tableau files processed!")


if __name__ == "__main__":
    main()
