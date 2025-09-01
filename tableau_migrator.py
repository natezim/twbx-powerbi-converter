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
        """Extract rich field metadata from XML."""
        if not self.xml_root:
            return {}
        
        field_metadata = {}
        
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
                            'table_name': value.split('.')[0] if '.' in value else value
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
                                    'table_name': parent_table
                                }
                
                break
        
        return field_metadata
    
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
                    'table_reference': metadata.get('table_reference', field_name)
                }
                ds_info['fields'].append(field_info)
            
            ds_info['field_count'] = len(ds_info['fields'])
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
            
            # Write CSV with field mapping
            with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['Original_Field_Name', 'Tableau_Field_Name', 'Data_Type', 'Table_Name', 'Table_Reference_SQL']
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
                    
                    writer.writerow({
                        'Original_Field_Name': original_name,
                        'Tableau_Field_Name': tableau_name,
                        'Data_Type': field['datatype'],
                        'Table_Name': field['table_name'],
                        'Table_Reference_SQL': table_ref_sql
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
        """Save one .sql file per data source with clean, practical structure."""
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
                
                # Field information
                if ds['fields']:
                    f.write("-- AVAILABLE FIELDS\n")
                    f.write("-- " + "-"*30 + "\n")
                    for field in ds['fields'][:20]:  # Show first 20 fields
                        f.write(f"-- {field['name']} ({field['datatype']}) - {field['role']} - Table: {field['table_name']}\n")
                    
                    if ds['field_count'] > 20:
                        f.write(f"-- ... and {ds['field_count'] - 20} more fields\n")
                    f.write("\n")
                
                # SQL queries
                f.write("-- SQL QUERIES\n")
                f.write("-- " + "="*30 + "\n\n")
                
                sql_queries = self.generate_practical_sql(ds)
                for i, query in enumerate(sql_queries, 1):
                    f.write(f"-- {query['name']} ({query['type']})\n")
                    f.write("-- " + "-"*30 + "\n")
                    f.write(query['sql'])
                    f.write("\n\n")
                
                # Power BI instructions
                f.write("-- POWER BI MIGRATION INSTRUCTIONS\n")
                f.write("-- " + "="*30 + "\n")
                if ds['connections'] and any(conn['dbclass'] in ['postgres', 'sqlserver', 'oracle', 'mysql'] for conn in ds['connections']):
                    f.write("-- 1. Use the connection details above to connect to your database\n")
                    f.write("-- 2. Import the tables you need\n")
                    f.write("-- 3. Create relationships in Power BI Model view\n")
                    f.write("-- 4. Use the field mapping CSV for column mapping\n")
                elif ds['connections'] and any(conn['filename'] for conn in ds['connections']):
                    f.write("-- 1. Import the files directly into Power BI\n")
                    f.write("-- 2. Create relationships in Power BI Model view\n")
                    f.write("-- 3. Use the field mapping CSV for column mapping\n")
                else:
                    f.write("-- 1. Connect to your data source\n")
                    f.write("-- 2. Import the fields you need\n")
                    f.write("-- 3. Create relationships in Power BI as needed\n")
                    f.write("-- 4. Use the field mapping CSV for column mapping\n")
            
            print(f"✅ Created: {sql_file}")


def main():
    """Command line interface for testing."""
    print("🔍 Tableau Data Source Extractor (Official API + XML Metadata)")
    print("=" * 70)
    
    # Test with NFL Dashboard
    nfl_file = "NFL Dashboard.twbx"
    
    if not os.path.exists(nfl_file):
        print(f"❌ File not found: {nfl_file}")
        print("Please place the NFL Dashboard.twbx file in the current directory")
        return
    
    print(f"📂 Processing: {nfl_file}")
    
    try:
        extractor = TableauDataSourceExtractor(nfl_file)
        
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
            
            print("✅ Extraction complete!")
            
        else:
            print("❌ Failed to parse TWBX file")
    
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
