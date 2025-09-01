#!/usr/bin/env python3
"""
Simple Tableau Data Source Extractor
Using Official Tableau Document API + XML Metadata
"""

import os
import csv
import xml.etree.ElementTree as ET
from tableaudocumentapi import Workbook


class TableauDataSourceExtractor:
    def __init__(self, twbx_path):
        self.twbx_path = twbx_path
        self.workbook = None
        self.xml_root = None
        
    def extract_and_parse(self):
        """Use official Tableau API + extract XML for rich metadata."""
        try:
            self.workbook = Workbook(self.twbx_path)
            
            # Also extract the XML for rich field metadata
            import zipfile
            with zipfile.ZipFile(self.twbx_path, 'r') as z:
                # Find the .twb file
                twb_files = [f for f in z.namelist() if f.endswith('.twb')]
                if twb_files:
                    twb_content = z.read(twb_files[0]).decode('utf-8')
                    self.xml_root = ET.fromstring(twb_content)
            
            return True
        except Exception as e:
            print(f"❌ Failed to parse: {e}")
            return False
    
    def extract_field_metadata_from_xml(self, datasource_name):
        """Extract rich field metadata from XML including usage tracking."""
        if not self.xml_root:
            return {}
        
        field_metadata = {}
        used_fields = set()
        
        # Find the datasource in XML
        for datasource in self.xml_root.findall('.//datasource'):
            if datasource.get('name') == datasource_name:
                # Extract field mappings from <cols> section
                cols_section = datasource.find('.//cols')
                if cols_section is not None:
                    for col_map in cols_section.findall('map'):
                        key = col_map.get('key', '').replace('[', '').replace(']', '')
                        value = col_map.get('value', '').replace('[', '').replace(']', '')
                        field_metadata[key] = {
                            'table_reference': value,
                            'table_name': value.split('.')[0] if '.' in value else value,
                            'used_in_workbook': False  # Will be updated below
                        }
                
                # Extract detailed metadata from <metadata-records> section
                metadata_section = datasource.find('.//metadata-records')
                if metadata_section is not None:
                    for record in metadata_section.findall("metadata-record[@class='column']"):
                        local_name = record.find('local-name')
                        if local_name is not None:
                            field_name = local_name.text.replace('[', '').replace(']', '')
                            
                            # Get data type
                            local_type = record.find('local-type')
                            data_type = local_type.text if local_type is not None else 'Unknown'
                            
                            # Get aggregation/role
                            aggregation = record.find('aggregation')
                            role = aggregation.text if aggregation is not None else 'None'
                            
                            # Get parent table
                            parent_name = record.find('parent-name')
                            parent_table = parent_name.text.replace('[', '').replace(']', '') if parent_name is not None else 'Unknown'
                            
                            # Get remote name (original database field)
                            remote_name = record.find('remote-name')
                            remote_field = remote_name.text if remote_name is not None else field_name
                            
                            # Update field metadata with rich information
                            if field_name in field_metadata:
                                field_metadata[field_name].update({
                                    'data_type': data_type,
                                    'role': role,
                                    'parent_table': parent_table,
                                    'remote_name': remote_field
                                })
                            else:
                                field_metadata[field_name] = {
                                    'data_type': data_type,
                                    'role': role,
                                    'parent_table': parent_table,
                                    'remote_name': remote_field,
                                    'table_reference': f'{parent_table}.{remote_field}',
                                    'table_name': parent_table,
                                    'used_in_workbook': False
                                }
                
                # Now check for field usage across the entire workbook
                print(f"   Debug: Checking usage for {len(field_metadata)} fields")
                self.track_field_usage(field_metadata)
                
                # Count used fields
                used_count = sum(1 for field in field_metadata.values() if field.get('used_in_workbook', False))
                print(f"   Debug: Found {used_count} fields used in workbook")
                
                break
        
        return field_metadata
    
    def track_field_usage(self, field_metadata):
        """Track which fields are actually used in the workbook using official Tableau API."""
        if not hasattr(self, 'workbook') or not self.workbook:
            return
        
        # Use the official Tableau API to check field usage
        for datasource in self.workbook.datasources:
            # According to API docs, datasource.fields returns key-value pairs
            if isinstance(datasource.fields, dict):
                for field_name, field_attrs in datasource.fields.items():
                    # Check if this field is used in any worksheets
                    if hasattr(field_attrs, 'worksheets') and field_attrs.worksheets:
                        # Try to match this field with our metadata
                        # Clean the field name (remove brackets and extra info)
                        clean_field_name = field_name.replace('[', '').replace(']', '')
                        
                        # Look for exact matches first
                        if clean_field_name in field_metadata:
                            field_metadata[clean_field_name]['used_in_workbook'] = True
                        else:
                            # Try partial matches (field name contains our metadata key)
                            for metadata_key in field_metadata.keys():
                                if metadata_key in clean_field_name or clean_field_name in metadata_key:
                                    field_metadata[metadata_key]['used_in_workbook'] = True
                                    break
    
    def find_datasource_xml(self, datasource_name):
        """Find the XML element for a specific datasource."""
        if not self.xml_root:
            return None
        
        for datasource in self.xml_root.findall('.//datasource'):
            if datasource.get('name') == datasource_name:
                return datasource
        return None
    
    def extract_sql_from_tableau_xml(self, datasource_xml):
        """Extract all SQL-related information from Tableau XML using only standard library."""
        sql_info = {
            'custom_sql': [],
            'table_references': [],
            'relationships': [],
            'connection_details': {}
        }
        
        # 1. CUSTOM SQL (Priority #1 - this is usually what teams need most)
        for relation in datasource_xml.findall('.//relation[@type="text"]'):
            if relation.text and relation.text.strip():
                sql_info['custom_sql'].append({
                    'name': relation.get('name', 'Custom Query'),
                    'sql': relation.text.strip(),
                    'connection': relation.get('connection', '')
                })
        
        # 2. TABLE REFERENCES (Direct table access)
        for relation in datasource_xml.findall('.//relation[@table]'):
            table_name = relation.get('table', '')
            if table_name:
                sql_info['table_references'].append({
                    'table': table_name,
                    'connection': relation.get('connection', ''),
                    'type': relation.get('type', 'table')
                })
        
        # 3. EXTRACT ALL TABLES AND THEIR RELATIONSHIPS
        all_tables = {}  # alias -> {table_name, connection}
        all_relationships = []
        
        # First pass: collect all tables and their aliases
        for relation in datasource_xml.findall('.//relation[@table]'):
            table_name = relation.get('table', '')
            table_alias = relation.get('name', '')
            connection = relation.get('connection', '')
            
            if table_name and table_alias:
                all_tables[table_alias] = {
                    'table_name': table_name,
                    'connection': connection
                }
        
        # Second pass: extract unique relationships
        seen_relationships = set()
        
        for relation in datasource_xml.findall('.//relation[@type="join"]'):
            # Extract join clauses for this relation
            join_clauses = []
            
            for clause in relation.findall('.//clause[@type="join"]'):
                for expr in clause.findall('.//expression[@op="="]'):
                    expressions = expr.findall('expression')
                    if len(expressions) == 2:
                        left_field = expressions[0].get('op', '').replace('[', '').replace(']', '')
                        right_field = expressions[1].get('op', '').replace('[', '').replace(']', '')
                        if left_field and right_field:
                            join_clauses.append(f"{left_field} = {right_field}")
            
            # Create relationship signature
            if join_clauses:
                join_clauses.sort()
                relationship_key = '|'.join(join_clauses)
                
                if relationship_key not in seen_relationships:
                    seen_relationships.add(relationship_key)
                    
                    # Find tables involved in this relationship
                    tables_in_relation = set()
                    for clause in join_clauses:
                        for field in clause.split(' = '):
                            if '.' in field:
                                table_alias = field.split('.')[0]
                                if table_alias in all_tables:
                                    tables_in_relation.add(table_alias)
                    
                    if tables_in_relation:
                        all_relationships.append({
                            'join_type': relation.get('join', 'left'),
                            'clauses': join_clauses,
                            'tables': sorted(list(tables_in_relation))
                        })
        
        # Store the extracted information
        sql_info['all_tables'] = all_tables
        sql_info['relationships'] = all_relationships
        
        return sql_info
    
    def generate_migration_sql(self, sql_info, connection_info):
        """Generate practical SQL for Power BI migration."""
        migration_queries = []
        
        # Custom SQL (use as-is)
        for custom in sql_info['custom_sql']:
            migration_queries.append({
                'name': f"Custom Query: {custom['name']}",
                'sql': custom['sql'],
                'type': 'Custom SQL - Use directly in Power BI'
            })
        
        # Table imports (individual SELECT statements) - deduplicate
        unique_tables = set()
        for table_ref in sql_info['table_references']:
            table_name = table_ref['table']
            if table_name not in unique_tables:
                unique_tables.add(table_name)
                migration_queries.append({
                    'name': f"Import {table_name}",
                    'sql': f"SELECT * FROM {table_name};",
                    'type': 'Table Import'
                })
        
        # Show all tables used in a clean format
        if sql_info.get('all_tables'):
            tables_note = "-- TABLES TO IMPORT INTO POWER BI:\n"
            for alias, table_info in sorted(sql_info['all_tables'].items()):
                table_name = table_info['table_name'].replace('[public].', '')
                tables_note += f"-- {alias} = {table_name}\n"
            tables_note += "\n"
            
            migration_queries.append({
                'name': 'Tables Overview',
                'type': 'Instructions',
                'sql': tables_note
            })
        
        # Show clear base table and JOIN statements
        if sql_info['relationships']:
            relationship_note = "-- TABLEAU RELATIONSHIP STRUCTURE:\n"
            relationship_note += "SELECT * FROM nfl_dimers_lines\n"
            
            # Extract unique join conditions
            all_joins = set()
            for rel in sql_info['relationships']:
                all_joins.update(rel['clauses'])
            
            # Show each join as a proper JOIN statement
            for join in sorted(all_joins):
                if 'nfl_dimers_lines.' in join:
                    # This is a join FROM the base table
                    relationship_note += f"left join {join}\n"
                else:
                    # This is a join between other tables
                    relationship_note += f"left join {join}\n"
            
            relationship_note += "\n-- POWER BI SETUP:\n"
            relationship_note += "-- 1. Import all tables above\n"
            relationship_note += "-- 2. Create relationships in Model view using the JOIN conditions above\n"
            relationship_note += "-- 3. Set cardinality: Many (nfl_dimers_lines) to One (other tables)\n"
            
            migration_queries.append({
                'name': 'Power BI Relationship Setup',
                'type': 'Instructions',
                'sql': relationship_note
            })
        
        return migration_queries
    
    def extract_data_sources(self):
        """Extract data sources using official Tableau API + XML metadata."""
        if not self.workbook:
            return []
        
        data_sources = []
        
        # Iterate through data sources using official API
        for datasource in self.workbook.datasources:
            ds_info = {
                'name': datasource.name,
                'caption': datasource.caption,
                'connections': [],
                'fields': [],
                'field_count': 0
            }
            
            # Get connections using official API - handles federated sources correctly
            for connection in datasource.connections:
                conn_info = {
                    'server': getattr(connection, 'server', ''),
                    'dbname': getattr(connection, 'dbname', ''),
                    'username': getattr(connection, 'username', ''),
                    'dbclass': getattr(connection, 'dbclass', ''),  # postgres, sqlserver, etc
                    'port': getattr(connection, 'port', ''),
                    'schema': getattr(connection, 'schema', ''),
                    'filename': getattr(connection, 'filename', '')
                }
                ds_info['connections'].append(conn_info)
            
            # Get rich field metadata from XML
            xml_metadata = self.extract_field_metadata_from_xml(datasource.name)
            print(f"   Found {len(xml_metadata)} fields with rich metadata from XML")
            
            # Get SQL queries from XML using pure parsing
            datasource_xml = self.find_datasource_xml(datasource.name)
            if datasource_xml:
                sql_info = self.extract_sql_from_tableau_xml(datasource_xml)
                migration_queries = self.generate_migration_sql(sql_info, ds_info['connections'])
                print(f"   Found {len(sql_info['custom_sql'])} custom SQL queries")
                print(f"   Found {len(sql_info['table_references'])} table references")
                print(f"   Found {len(sql_info['relationships'])} relationships")
            else:
                migration_queries = []
                sql_info = {'custom_sql': [], 'table_references': [], 'relationships': []}
            
            # Create comprehensive field information
            for field_name, metadata in xml_metadata.items():
                field_info = {
                    'name': field_name,
                    'caption': field_name,  # Use field name as caption for now
                    'datatype': metadata.get('data_type', 'Unknown'),
                    'role': metadata.get('role', 'None'),
                    'id': field_name,
                    'table_name': metadata.get('table_name', 'Unknown'),
                    'remote_name': metadata.get('remote_name', field_name),
                    'table_reference': metadata.get('table_reference', field_name),
                    'used_in_workbook': metadata.get('used_in_workbook', False)  # Include usage tracking
                }
                ds_info['fields'].append(field_info)
            
            ds_info['field_count'] = len(ds_info['fields'])
            ds_info['sql_queries'] = migration_queries
            ds_info['sql_info'] = sql_info
            data_sources.append(ds_info)
        
        return data_sources
    
    def export_field_mapping_csv(self, output_dir, data_sources):
        """Export field mapping to CSV for Power BI migration."""
        os.makedirs(output_dir, exist_ok=True)
        
        for ds in data_sources:
            if not ds['fields']:
                continue
            
            # Create CSV filename
            safe_name = ds['caption'].replace(' ', '_').replace('/', '_')
            safe_name = ''.join(c for c in safe_name if c.isalnum() or c in '_-')
            csv_file = os.path.join(output_dir, f"{safe_name}_field_mapping.csv")
            
            # Sort fields by table name first, then by column name
            sorted_fields = sorted(ds['fields'], key=lambda x: (x['table_name'], x['remote_name']))
            
            # Write CSV with field mapping including usage
            with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['Original_Field_Name', 'Tableau_Field_Name', 'Data_Type', 'Table_Name', 'Table_Reference_SQL', 'Used_In_Workbook']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                # Write header
                writer.writeheader()
                
                # Write field mapping data in sorted order
                for field in sorted_fields:
                    # Clean up field names for better readability
                    original_name = field['remote_name']
                    tableau_name = field['name']
                    
                    # Add quotes around table reference for easy SQL copy-paste
                    table_ref_sql = f"'{field['table_reference']}'"
                    
                    # Mark if field is used in the workbook
                    used_status = 'Yes' if field.get('used_in_workbook', False) else 'No'
                    
                    writer.writerow({
                        'Original_Field_Name': original_name,
                        'Tableau_Field_Name': tableau_name,
                        'Data_Type': field['datatype'],
                        'Table_Name': field['table_name'],
                        'Table_Reference_SQL': table_ref_sql,
                        'Used_In_Workbook': used_status
                    })
            
            print(f"✅ Created field mapping CSV: {csv_file}")
    
    def generate_sql_from_connections(self, connections):
        """Generate simple connection-based SQL."""
        sql_queries = []
        
        for conn in connections:
            if conn['dbclass'] in ['postgres', 'sqlserver', 'oracle', 'mysql']:
                # Database connections - let user specify tables
                sql_queries.append({
                    'name': f"Connect to {conn['dbclass']}",
                    'sql': f"""-- Connection: {conn['dbclass']}
-- Server: {conn['server']}
-- Database: {conn['dbname']}
-- Port: {conn['port']}
-- Schema: {conn['schema']}

-- Use this connection info in Power BI to connect
-- Then import tables as needed
SELECT * FROM your_table_name;""",
                    'type': 'Connection Template'
                })
            elif conn['filename']:
                # File-based connections (Excel, CSV, etc.)
                sql_queries.append({
                    'name': f"Import {conn['filename']}",
                    'sql': f"""-- File: {conn['filename']}
-- Import this file directly into Power BI
-- No SQL needed - use Power BI's file import""",
                    'type': 'File Import'
                })
        
        return sql_queries
    
    def generate_practical_sql(self, data_source):
        """Generate practical SQL based on data source type."""
        sql_queries = []
        
        # Generate connection-based SQL
        if data_source['connections']:
            sql_queries.extend(self.generate_sql_from_connections(data_source['connections']))
        
        # Add Power BI setup instructions
        if data_source['field_count'] > 0:
            setup_instructions = {
                'name': 'Power BI Setup Instructions',
                'sql': f"""-- POWER BI SETUP INSTRUCTIONS
-- Data Source: {data_source['caption']}
-- Fields Available: {data_source['field_count']}

-- 1. Use the connection information above to connect to your data source
-- 2. Import the tables/fields you need
-- 3. Create relationships in Power BI Model view
-- 4. Let Power BI handle the join logic automatically

-- Available Fields: {', '.join([f['name'] for f in data_source['fields'][:10]])}
{f'-- ... and {data_source["field_count"] - 10} more fields' if data_source['field_count'] > 10 else ''}""",
                'type': 'Instructions'
            }
            sql_queries.insert(0, setup_instructions)
        
        return sql_queries
    
    def export_sql_files(self, output_dir, data_sources):
        """Save one .sql file per data source with clean, simple structure."""
        os.makedirs(output_dir, exist_ok=True)
        
        for ds in data_sources:
            if not ds['connections'] and not ds['fields']:
                continue
            
            safe_name = ds['caption'].replace(' ', '_').replace('/', '_')
            safe_name = ''.join(c for c in safe_name if c.isalnum() or c in '_-')
            
            sql_file = os.path.join(output_dir, f"{safe_name}.sql")
            
            with open(sql_file, 'w', encoding='utf-8') as f:
                f.write(f"-- Data Source: {ds['name']}\n")
                f.write(f"-- Caption: {ds['caption']}\n")
                f.write(f"-- Fields Available: {ds['field_count']}\n")
                f.write("-- " + "="*50 + "\n\n")
                
                # Connection details
                if ds['connections']:
                    f.write("-- CONNECTION DETAILS\n")
                    f.write("-- " + "-"*30 + "\n")
                    for i, conn in enumerate(ds['connections'], 1):
                        f.write(f"-- Connection {i}:\n")
                        for key, value in conn.items():
                            if value:
                                f.write(f"--   {key}: {value}\n")
                        f.write("\n")
                
                # SQL Queries from Tableau
                if ds.get('sql_queries'):
                    f.write("-- SQL QUERIES FROM TABLEAU\n")
                    f.write("-- " + "-"*30 + "\n")
                    for i, query in enumerate(ds['sql_queries'], 1):
                        f.write(f"-- Query {i}: {query['type']}\n")
                        f.write(query['sql'])
                        f.write("\n\n")
                
                # Simple Power BI instructions
                f.write("-- POWER BI SETUP\n")
                f.write("-- " + "-"*30 + "\n")
                if ds['connections'] and any(conn['dbclass'] in ['postgres', 'sqlserver', 'oracle', 'mysql'] for conn in ds['connections']):
                    f.write("-- 1. Use connection details above to connect to database\n")
                    f.write("-- 2. Import tables as needed\n")
                    f.write("-- 3. Use field mapping CSV for column details\n")
                    if ds.get('sql_queries'):
                        f.write("-- 4. Use SQL queries above as reference for data structure\n")
                elif ds['connections'] and any(conn['filename'] for conn in ds['connections']):
                    f.write("-- 1. Import files directly into Power BI\n")
                    f.write("-- 2. Use field mapping CSV for column details\n")
                else:
                    f.write("-- 1. Connect to data source\n")
                    f.write("-- 2. Use field mapping CSV for column details\n")
            
            print(f"✅ Created: {sql_file}")


def main():
    """Command line interface for testing."""
    print("🔍 Tableau Data Source Extractor (Official API + XML Metadata)")
    print("=" * 70)
    
    # Find all TWBX files in current directory
    twbx_files = [f for f in os.listdir('.') if f.endswith('.twbx')]
    
    if not twbx_files:
        print("❌ No .twbx files found in current directory")
        print("Please place one or more .twbx files in the current directory")
        return
    
    print(f"📂 Found {len(twbx_files)} TWBX file(s): {', '.join(twbx_files)}")
    print()
    
    # Process each TWBX file
    for twbx_file in twbx_files:
        print(f"🔄 Processing: {twbx_file}")
        print("-" * 50)
        
        try:
            extractor = TableauDataSourceExtractor(twbx_file)
            
            if extractor.extract_and_parse():
                print("✅ TWBX parsed successfully using official Tableau API + XML")
                
                print("🔍 Analyzing data sources...")
                data_sources = extractor.extract_data_sources()
                
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
                
                print("💾 Exporting SQL files...")
                extractor.export_sql_files("output", data_sources)
                
                print("📊 Exporting field mapping CSV...")
                extractor.export_field_mapping_csv("output", data_sources)
                
                print(f"✅ Extraction complete for {twbx_file}!")
                
            else:
                print(f"❌ Failed to parse {twbx_file}")
        
        except Exception as e:
            print(f"❌ Error processing {twbx_file}: {e}")
            import traceback
            traceback.print_exc()
        
        print()  # Add spacing between files
    
    print("🎉 All TWBX files processed!")


if __name__ == "__main__":
    main()
