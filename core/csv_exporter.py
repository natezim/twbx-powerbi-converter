#!/usr/bin/env python3
"""
CSV Exporter Module
Handles export of field mapping data to CSV format
"""

import os
import csv
from utils.file_utils import create_safe_filename


class CSVExporter:
    """Exports field mapping to CSV for Power BI migration."""
    
    def __init__(self):
        pass
    
    def get_actual_table_name(self, alias, datasource_info):
        """Get the actual table name from an alias."""
        if datasource_info.get('sql_info', {}).get('all_tables'):
            for table_alias, table_info in datasource_info['sql_info']['all_tables'].items():
                # Check if the alias matches exactly or if it's a close match
                if table_alias == alias or table_alias.replace(' ', '_') == alias or table_alias == alias.replace('_', ' '):
                    return table_info['table_name'].replace('[', '').replace(']', '').replace(' ', '_')
        return alias  # Return the alias if no mapping found
    
    def get_original_alias(self, processed_alias, datasource_info):
        """Get the original Tableau alias (with spaces) from a processed alias."""
        if datasource_info.get('sql_info', {}).get('all_tables'):
            for table_alias, table_info in datasource_info['sql_info']['all_tables'].items():
                # Check if the processed alias matches the table alias (with or without spaces)
                if (table_alias == processed_alias or 
                    table_alias.replace(' ', '_') == processed_alias or 
                    table_alias == processed_alias.replace('_', ' ')):
                    return table_alias  # Return the original alias with spaces
        return processed_alias  # Return the processed alias if no mapping found
    
    def export_field_mapping_csv(self, output_dir, data_sources):
        """Export field mapping to CSV for Power BI migration."""
        # Create workbook-specific folder
        if data_sources:
            workbook_name = data_sources[0].get('workbook_name', 'Unknown')
            safe_workbook = workbook_name.replace(' ', '_').replace('/', '_')
            safe_workbook = ''.join(c for c in safe_workbook if c.isalnum() or c in '_-')
            workbook_folder = os.path.join(output_dir, safe_workbook)
            os.makedirs(workbook_folder, exist_ok=True)
            
            # Copy the TWBX file into the workbook folder for a complete migration package
            try:
                import shutil
                twbx_path = data_sources[0].get('twbx_path', '')
                if twbx_path and os.path.exists(twbx_path):
                    twbx_filename = os.path.basename(twbx_path)
                    twbx_dest = os.path.join(workbook_folder, twbx_filename)
                    shutil.copy2(twbx_path, twbx_dest)
                    print(f"✅ Copied TWBX file to: {twbx_dest}")
            except Exception as e:
                print(f"⚠️  Warning: Could not copy TWBX file: {e}")
        else:
            workbook_folder = output_dir
        
        # For single datasource workbooks, use simple filenames
        if len(data_sources) == 1:
            csv_file = os.path.join(workbook_folder, "field_mapping.csv")
            ds = data_sources[0]
            if ds['fields']:
                self._write_field_mapping_csv(csv_file, ds)
                print(f"✅ Created field mapping CSV: {csv_file}")
        else:
            # Multiple datasources - use datasource names
            for ds in data_sources:
                if not ds['fields']:
                    continue
                
                datasource_name = ds.get('caption') or ds.get('name') or 'Unknown'
                safe_datasource = datasource_name.replace(' ', '_').replace('/', '_')
                safe_datasource = ''.join(c for c in safe_datasource if c.isalnum() or c in '_-')
                csv_file = os.path.join(workbook_folder, f"{safe_datasource}_field_mapping.csv")
                
                self._write_field_mapping_csv(csv_file, ds)
                print(f"✅ Created field mapping CSV: {csv_file}")

    def _write_field_mapping_csv(self, csv_file, ds):
        """Helper method to write field mapping CSV for a single datasource."""
        # Sort fields by table name first, then by column name
        # Put calculated fields at the end since they don't have table names
        sorted_fields = sorted(ds['fields'], key=lambda x: (
            x.get('is_calculated', False),  # Calculated fields last
            x.get('table_name', ''), 
            x.get('remote_name', '')
        ))
        
        # Debug: Show calculated fields and parameters
        calc_fields = [f for f in ds['fields'] if f.get('is_calculated', False)]
        param_fields = [f for f in ds['fields'] if f.get('is_parameter', False)]
        print(f"   CSV Export: Found {len(calc_fields)} calculated fields: {[f.get('name', 'Unknown') for f in calc_fields]}")
        if param_fields:
            print(f"   CSV Export: Found {len(param_fields)} parameters: {[f.get('name', 'Unknown') for f in param_fields]}")
        
        # Write CSV with field mapping including usage and calculated field info
        with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['Original_Field_Name', 'Tableau_Field_Name', 'Data_Type', 'Table_Name', 'Table_Reference_SQL', 'Used_In_Workbook', 'Type', 'Calculation_Formula']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header
            writer.writeheader()
            
            # Write field mapping data in sorted order
            for field in sorted_fields:
                # Clean up field names for better readability
                original_name = field.get('remote_name', '') or ''
                original_name = original_name.strip() if original_name else ''
                
                # For tableau_name, prioritize alias/caption over internal name for calculated fields
                is_calculated = field.get('is_calculated', False)
                if is_calculated:
                    # For calculated fields, prefer caption or api_caption over the internal name
                    tableau_name = (field.get('caption') or 
                                  field.get('api_caption') or 
                                  field.get('name', ''))
                else:
                    # For regular fields, use the standard name
                    tableau_name = field.get('name', '') or ''
                tableau_name = tableau_name.strip() if tableau_name else ''
                is_parameter = field.get('is_parameter', False)
                calculation_formula = field.get('calculation_formula', '') or ''
                
                # For calculated fields, we might not have original_name
                if is_calculated and not original_name:
                    original_name = tableau_name  # Use tableau name as original for calculated fields
                
                # Ensure we have valid data
                if not tableau_name:
                    continue
                
                # Format table reference as 'Table.field_name as tableau_name'
                table_ref = field.get('table_reference', '') or ''
                table_ref = table_ref.strip() if table_ref else ''
                
                # Special handling for parameters
                if is_parameter:
                    # For parameters, show parameter type and current value
                    param_value = field.get('calculation_formula', '')
                    if param_value:
                        table_ref_sql = f"PARAMETER: {tableau_name} = {param_value}"
                    else:
                        table_ref_sql = f"PARAMETER: {tableau_name}"
                elif is_calculated:
                    # For calculated fields, show the actual Tableau field name
                    tableau_display_name = field.get('name', tableau_name)
                    table_ref_sql = f"CALCULATED: {tableau_display_name}"
                elif table_ref and original_name and not is_calculated:
                    if table_ref != tableau_name:
                        # Field was renamed in Tableau - add quotes around tableau_name if it has spaces
                        if ' ' in tableau_name:
                            table_ref_sql = f"{table_ref}.{original_name} as '{tableau_name}'"
                        else:
                            table_ref_sql = f"{table_ref}.{original_name} as {tableau_name}"
                    else:
                        # Field wasn't renamed, just use original
                        table_ref_sql = f"{table_ref}.{original_name}"
                else:
                    # Use table_name as fallback
                    table_name = field.get('table_name', 'Unknown') or 'Unknown'
                    table_ref_sql = f"{table_name}.{original_name}"
                
                # Clean up any special characters that might cause CSV issues
                table_ref_sql = table_ref_sql.replace('"', '').replace("'", '').replace('\n', ' ').replace('\r', ' ')
                
                # Mark if field is used in the workbook
                used_status = 'Yes' if field.get('used_in_workbook', False) else 'No'
                
                # Determine field type
                if is_parameter:
                    field_type = 'Parameter'
                elif is_calculated:
                    field_type = 'Calculated Field'
                else:
                    field_type = 'Column'
                
                # Clean up calculation formula for CSV
                clean_formula = calculation_formula.replace('\n', ' ').replace('\r', ' ').replace('"', "'") if calculation_formula else ''
                
                # For calculated fields, use "Workbook" as table name instead of "Unknown"
                table_name_for_csv = field.get('table_name', 'Unknown')
                if is_calculated and (table_name_for_csv == 'Unknown' or not table_name_for_csv):
                    table_name_for_csv = 'Workbook'
                
                writer.writerow({
                    'Original_Field_Name': original_name,
                    'Tableau_Field_Name': tableau_name,
                    'Data_Type': field.get('datatype', 'Unknown'),
                    'Table_Name': table_name_for_csv,
                    'Table_Reference_SQL': table_ref_sql,
                    'Used_In_Workbook': used_status,
                    'Type': field_type,
                    'Calculation_Formula': clean_formula
                })

    def export_setup_guide_txt(self, output_dir, data_sources):
        """Export a simple text setup guide for Power BI migration."""
        # Create workbook-specific folder
        if data_sources:
            workbook_name = data_sources[0].get('workbook_name', 'Unknown')
            safe_workbook = workbook_name.replace(' ', '_').replace('/', '_')
            safe_workbook = ''.join(c for c in safe_workbook if c.isalnum() or c in '_-')
            workbook_folder = os.path.join(output_dir, safe_workbook)
            os.makedirs(workbook_folder, exist_ok=True)
        else:
            workbook_folder = output_dir
        
        for datasource_info in data_sources:
            # Create filename using datasource name only (since we're in workbook folder)
            # Use name if caption is empty, fallback to 'Unknown' if both are empty
            datasource_name = datasource_info.get('caption') or datasource_info.get('name') or 'Unknown'
            safe_datasource = datasource_name.replace(' ', '_').replace('/', '_')
            safe_datasource = ''.join(c for c in safe_datasource if c.isalnum() or c in '_-')
            txt_filename = f"{safe_datasource}_setup_guide.txt"
            txt_path = os.path.join(workbook_folder, txt_filename)
            
            with open(txt_path, 'w', encoding='utf-8') as f:
                f.write(f"POWER BI SETUP GUIDE\n")
                f.write(f"==================\n\n")
                f.write(f"Data Source: {datasource_info['name']}\n")
                f.write(f"Caption: {datasource_info.get('caption', 'N/A')}\n")
                f.write(f"Fields Available: {datasource_info.get('field_count', 0)}\n")
                
                # Count used fields and calculated fields
                used_fields = sum(1 for field in datasource_info.get('fields', []) if field.get('used_in_workbook', False))
                calculated_fields = sum(1 for field in datasource_info.get('fields', []) if field.get('is_calculated', False))
                parameter_fields = sum(1 for field in datasource_info.get('fields', []) if field.get('is_parameter', False))
                f.write(f"Fields Used in Workbook: {used_fields}\n")
                f.write(f"Calculated Fields: {calculated_fields}\n")
                f.write(f"Parameter Fields: {parameter_fields}\n")
                
                # Add parameters section if any exist
                if parameter_fields > 0:
                    f.write(f"\nPARAMETERS TO RECREATE IN POWER BI:\n")
                    f.write(f"----------------------------------\n")
                    for field in datasource_info.get('fields', []):
                        if field.get('is_parameter', False):
                            param_name = field.get('name', 'Unknown')
                            param_value = field.get('calculation_formula', '')
                            param_type = field.get('datatype', 'Unknown')
                            f.write(f"  📊 {param_name}:\n")
                            f.write(f"     Type: {param_type}\n")
                            if param_value:
                                f.write(f"     Default Value: {param_value}\n")
                            f.write(f"     Usage: Create as Power BI parameter\n\n")
                
                # Add hyper data information if available
                if datasource_info.get('hyper_data'):
                    hyper_tables = len(datasource_info['hyper_data'])
                    total_rows = sum(table_info['row_count'] for table_info in datasource_info['hyper_data'].values())
                    f.write(f"Hyper Data Tables: {hyper_tables}\n")
                    f.write(f"Total Data Rows: {total_rows:,}\n")
                
                f.write(f"\n")
                
                # Connection details
                if datasource_info.get('connections'):
                    f.write(f"CONNECTION DETAILS:\n")
                    f.write(f"------------------\n")
                    for i, conn in enumerate(datasource_info['connections'], 1):
                        f.write(f"Connection {i}:\n")
                        
                        # Standard connection properties
                        f.write(f"  Server: {conn.get('server', 'N/A')}\n")
                        f.write(f"  Database: {conn.get('dbname', 'N/A')}\n")
                        f.write(f"  Username: {conn.get('username', 'N/A')}\n")
                        f.write(f"  Type: {conn.get('dbclass', 'N/A')}\n")
                        f.write(f"  Port: {conn.get('port', 'N/A')}\n")
                        
                        # BigQuery-specific properties
                        if conn.get('dbclass') == 'bigquery':
                            if conn.get('project'):
                                f.write(f"  Project: {conn.get('project')}\n")
                            if conn.get('dataset'):
                                f.write(f"  Dataset: {conn.get('dataset')}\n")
                            if conn.get('location'):
                                f.write(f"  Location: {conn.get('location')}\n")
                            if conn.get('region'):
                                f.write(f"  Region: {conn.get('region')}\n")
                        
                        # Other cloud database properties
                        if conn.get('region') and conn.get('dbclass') != 'bigquery':
                            f.write(f"  Region: {conn.get('region')}\n")
                        
                        f.write(f"\n")
                
                # Tables to import
                if datasource_info.get('sql_info', {}).get('all_tables'):
                    f.write(f"TABLES TO IMPORT:\n")
                    f.write(f"----------------\n")
                    for alias, table_info in sorted(datasource_info['sql_info']['all_tables'].items()):
                        table_name = table_info['table_name']
                        clean_table = table_name.replace('[', '').replace(']', '').replace(' ', '_')
                        if alias != clean_table:
                            f.write(f"  {clean_table} as {alias}\n")
                        else:
                            f.write(f"  {clean_table}\n")
                    f.write(f"\n")
                
                # Main table
                if datasource_info.get('sql_info', {}).get('all_tables'):
                    first_alias = list(datasource_info['sql_info']['all_tables'].keys())[0]
                    main_table = datasource_info['sql_info']['all_tables'][first_alias]['table_name']
                    clean_main_table = main_table.replace('[', '').replace(']', '').replace(' ', '_')
                    f.write(f"MAIN TABLE: {clean_main_table} (aliased as {first_alias})\n\n")
                
                # Hyper Data Tables section (if available)
                if datasource_info.get('hyper_data'):
                    f.write(f"HYPER DATA TABLES (Ready for Power BI Import):\n")
                    f.write(f"--------------------------------------------\n")
                    
                    for table_key, table_info in datasource_info['hyper_data'].items():
                        f.write(f"📊 {table_key}:\n")
                        f.write(f"   Source: {table_info['source_file']}\n")
                        f.write(f"   Table: {table_info['table_name']}\n")
                        f.write(f"   Rows: {table_info['row_count']:,}\n")
                        f.write(f"   Columns: {table_info['column_count']}\n")
                        f.write(f"   Columns: {', '.join(str(col) for col in table_info['columns'])}\n")
                        f.write(f"   Excel File: {table_key}.xlsx\n\n")
                    
                    f.write(f"💡 TIP: Import these Excel files directly into Power BI!\n")
                    f.write(f"   No need to recreate SQL queries - you have the actual data.\n\n")
                
                # Join conditions with types
                # Always initialize unique_relationships to avoid scope issues
                unique_relationships = {}
                
                if datasource_info.get('sql_info', {}).get('relationships') or datasource_info.get('sql_info', {}).get('join_conditions'):
                    f.write(f"CREATE THESE RELATIONSHIPS IN POWER BI MODEL VIEW:\n")
                    f.write(f"------------------------------------------------\n")
                    
                    # Process main relationships
                    if datasource_info['sql_info'].get('relationships'):
                        for rel in datasource_info['sql_info']['relationships']:
                            try:
                                join_type = rel.get('join_type', 'LEFT JOIN').upper()
                                
                                # Safely get conditions - handle missing or malformed data
                                conditions = rel.get('conditions', [])
                                if not isinstance(conditions, list):
                                    conditions = []
                                
                                # Create a unique key for this relationship
                                if conditions:
                                    conditions_key = '|'.join(sorted([str(c) for c in conditions if c]))
                                else:
                                    # If no conditions, use a different key
                                    left_table = rel.get('left_table', 'unknown')
                                    right_table = rel.get('right_table', 'unknown')
                                    conditions_key = f"{left_table}_{right_table}"
                                
                                if conditions_key and conditions_key not in unique_relationships:
                                    unique_relationships[conditions_key] = {
                                        'join_type': join_type,
                                        'conditions': conditions,
                                        'tables': rel.get('tables', []),
                                        'left_table': rel.get('left_table', ''),
                                        'right_table': rel.get('right_table', '')
                                    }
                            except Exception as e:
                                # Skip malformed relationships
                                print(f"Warning: Skipping malformed relationship: {e}")
                                continue
                    
                    # Process additional join conditions
                    additional_joins = datasource_info['sql_info'].get('join_conditions', [])
                    if isinstance(additional_joins, list):
                        for condition in additional_joins:
                            if condition and str(condition) not in [str(cond) for rel in unique_relationships.values() for cond in rel.get('conditions', [])]:
                                # This is a truly additional condition
                                unique_relationships[f"additional_{condition}"] = {
                                    'join_type': 'LEFT JOIN',
                                    'conditions': [condition],
                                    'tables': [],
                                    'left_table': '',
                                    'right_table': ''
                                }
                    
                    # Display unique relationships in simple SQL-like format
                    if unique_relationships:
                        for i, (key, rel) in enumerate(unique_relationships.items(), 1):
                            try:
                                join_type = rel['join_type']
                                
                                # Extract table names from conditions for simple display
                                if rel['conditions']:
                                    # Get the first condition to show the basic relationship
                                    first_condition = str(rel['conditions'][0])
                                    if '=' in first_condition:
                                        left_part, right_part = first_condition.split('=', 1)
                                        left_table = left_part.split('.')[0].strip()
                                        right_part = right_part.split('.')[0].strip()
                                        
                                        # Convert aliases to actual table names
                                        actual_left_table = self.get_actual_table_name(left_table, datasource_info)
                                        actual_right_table = self.get_actual_table_name(right_part, datasource_info)
                                        
                                        # Extract field names from the condition
                                        left_field = left_part.split('.')[1].strip() if '.' in left_part else ''
                                        right_field = right_part.split('.')[1].strip() if '.' in right_part else ''
                                        
                                        # Get original Tableau aliases (with spaces) from the table mapping
                                        left_original_alias = self.get_original_alias(left_table, datasource_info)
                                        right_original_alias = self.get_original_alias(right_part, datasource_info)
                                        
                                        # Format the relationship with AS for both tables
                                        # Add quotes around aliases with spaces
                                        left_alias = f'"{left_original_alias}"' if ' ' in left_original_alias else left_original_alias
                                        right_alias = f'"{right_original_alias}"' if ' ' in right_original_alias else right_alias
                                        f.write(f"{i}. {join_type} JOIN {actual_left_table} AS {left_alias} ON {actual_left_table}.{left_field} = {actual_right_table} AS {right_alias}.{right_field}\n")
                                    else:
                                        f.write(f"{i}. {join_type} relationship: {first_condition}\n")
                                else:
                                    # Fallback to basic table info if no conditions
                                    left_table = rel.get('left_table', 'unknown')
                                    right_table = rel.get('right_table', 'unknown')
                                    if left_table and right_table:
                                        f.write(f"{i}. {join_type} relationship between {left_table} and {right_table}\n")
                                    else:
                                        f.write(f"{i}. {join_type} relationship\n")
                            except Exception as e:
                                # Skip problematic relationships
                                print(f"Warning: Error processing relationship {i}: {e}")
                                continue
                    
                    # No fluff - just the relationships
                if not unique_relationships:
                    f.write(f"No relationships found\n")
                
                # SQL-ready column list for used fields (skip calculated fields)
                used_fields = [field for field in datasource_info.get('fields', []) 
                             if field.get('used_in_workbook', False) and 
                             field.get('table_reference') and  # Must have a table reference (not calculated)
                             field.get('remote_name')]  # Must have an original field name
                
                # Separate parameters and calculated fields
                parameter_fields = [field for field in datasource_info.get('fields', []) 
                                  if field.get('is_parameter', False) and 
                                  field.get('used_in_workbook', False)]
                
                calculated_fields = [field for field in datasource_info.get('fields', []) 
                                   if field.get('is_calculated', False) and 
                                   not field.get('is_parameter', False) and  # Exclude parameters
                                   field.get('used_in_workbook', False)]
                
                # Parameters section
                if parameter_fields:
                    f.write(f"\n")
                    f.write(f"PARAMETERS:\n")
                    f.write(f"-----------\n")
                    
                    # Sort parameter fields by name
                    sorted_param_fields = sorted(parameter_fields, key=lambda x: x.get('name', ''))
                    
                    for field in sorted_param_fields:
                        field_name = field.get('name', '').strip()
                        formula = field.get('calculation_formula', '').strip()
                        data_type = field.get('data_type', field.get('datatype', 'Unknown'))
                        
                        f.write(f"{field_name} (Parameter - {data_type}):\n")
                        f.write(f"  {formula}\n")
                        f.write(f"  {'-' * 50}\n\n")
                
                # Calculated fields section
                if calculated_fields:
                    f.write(f"\n")
                    f.write(f"CALCULATED FIELDS:\n")
                    f.write(f"------------------\n")
                    
                    # Sort calculated fields by name
                    sorted_calc_fields = sorted(calculated_fields, key=lambda x: x.get('name', ''))
                    
                    for field in sorted_calc_fields:
                        field_name = field.get('name', '').strip()
                        formula = field.get('calculation_formula', '').strip()
                        data_type = field.get('data_type', field.get('datatype', 'Unknown'))
                        role = field.get('role', 'Unknown')
                        aggregation = field.get('aggregation', 'Unknown')
                        
                        # Try to infer better data types and roles from the formula
                        if data_type == 'Unknown' and formula:
                            if 'DATEDIFF' in formula or 'DATETRUNC' in formula:
                                data_type = 'date'
                            elif 'SUM(' in formula or 'AVG(' in formula or 'COUNT(' in formula:
                                data_type = 'numeric'
                            elif 'IF' in formula or 'CASE' in formula or 'THEN' in formula:
                                data_type = 'string'
                        
                        if role == 'Unknown' and formula:
                            if 'SUM(' in formula or 'AVG(' in formula or 'COUNT(' in formula:
                                role = 'measure'
                            elif 'IF' in formula or 'CASE' in formula or 'THEN' in formula:
                                role = 'dimension'
                        
                        # For calculated fields, show the most meaningful information
                        if aggregation != 'Unknown' and aggregation != 'None':
                            f.write(f"{field_name} ({aggregation}):\n")
                        elif data_type != 'Unknown' and role != 'Unknown':
                            f.write(f"{field_name} ({data_type}, {role}):\n")
                        elif data_type != 'Unknown':
                            f.write(f"{field_name} ({data_type}):\n")
                        elif role != 'Unknown':
                            f.write(f"{field_name} ({role}):\n")
                        else:
                            f.write(f"{field_name}:\n")
                        
                        f.write(f"  {formula}\n")
                        f.write(f"  {'-' * 50}\n\n")
                
                if used_fields:
                    f.write(f"\n")
                    f.write(f"SQL COLUMNS:\n")
                    
                    # Group fields by table reference and sort by table name first, then field name
                    table_groups = {}
                    for field in used_fields:
                        table_ref = field.get('table_reference', '').strip()
                        
                        if table_ref not in table_groups:
                            table_groups[table_ref] = []
                        table_groups[table_ref].append(field)
                    
                    # Sort tables and fields within each table
                    for table_ref in sorted(table_groups.keys()):
                        # Add table header comment
                        f.write(f"\n-- {table_ref}:\n")
                        
                        # Sort fields within this table
                        sorted_fields = sorted(table_groups[table_ref], key=lambda x: x.get('remote_name', ''))
                        
                        for field in sorted_fields:
                            original_name = field.get('remote_name', '').strip()
                            
                            # For tableau_name, prioritize alias/caption for calculated fields
                            is_calculated = field.get('is_calculated', False)
                            if is_calculated:
                                tableau_name = (field.get('caption') or 
                                              field.get('api_caption') or 
                                              field.get('name', '')).strip()
                            else:
                                tableau_name = field.get('name', '').strip()
                            
                            # Format as 'Table.field_name as tableau_name'
                            # Quote table reference if it has spaces
                            quoted_table_ref = f'"{table_ref}"' if ' ' in table_ref else table_ref
                            if ' ' in tableau_name:
                                f.write(f"  {quoted_table_ref}.{original_name} as '{tableau_name}',\n")
                            else:
                                f.write(f"  {quoted_table_ref}.{original_name} as {tableau_name},\n")
            
            print(f"✅ Created setup guide: {txt_filename}")
        
        return True

    def export_dashboard_usage_csv(self, output_dir, data_sources, dashboard_info):
        """Export dashboard and worksheet usage information to CSV."""
        # Create workbook-specific folder
        if data_sources:
            workbook_name = data_sources[0].get('workbook_name', 'Unknown')
            safe_workbook = workbook_name.replace(' ', '_').replace('/', '_')
            safe_workbook = ''.join(c for c in safe_workbook if c.isalnum() or c in '_-')
            workbook_folder = os.path.join(output_dir, safe_workbook)
            os.makedirs(workbook_folder, exist_ok=True)
        else:
            workbook_folder = output_dir
        
        # For single datasource workbooks, use simple filenames
        if len(data_sources) == 1:
            csv_file = os.path.join(workbook_folder, "dashboard_usage.csv")
        else:
            # Multiple datasources - use datasource names
            for ds in data_sources:
                if not dashboard_info:
                    continue
                
                datasource_name = ds.get('caption') or ds.get('name') or 'Unknown'
                safe_datasource = datasource_name.replace(' ', '_').replace('/', '_')
                safe_datasource = ''.join(c for c in safe_datasource if c.isalnum() or c in '_-')
                csv_file = os.path.join(workbook_folder, f"{safe_datasource}_dashboard_usage.csv")
                
                self._write_dashboard_usage_csv(csv_file, dashboard_info)
                print(f"✅ Created dashboard usage CSV: {csv_file}")
            
            return  # Exit early for multiple datasources
        
        # Single datasource processing
        if dashboard_info:
            self._write_dashboard_usage_csv(csv_file, dashboard_info)
            print(f"✅ Created dashboard usage CSV: {csv_file}")

    def _write_dashboard_usage_csv(self, csv_file, dashboard_info):
        """Helper method to write dashboard usage CSV."""
        # Write CSV with dashboard and worksheet information
        with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['Item_Name', 'Item_Type', 'Chart_Type', 'Mark_Type', 'Size', 'Used_Fields', 'Filters', 'Filter_Function', 'Filter_Operation', 'Filter_Values', 'Filter_Description', 'Slicers', 'Rows_Layout', 'Columns_Layout', 'Cards_Layout', 'Aggregation', 'Power_BI_Recommendations']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header
            writer.writeheader()
            
            # Write dashboard and worksheet data
            for item_name, item_info in dashboard_info.items():
                item_type = item_info.get('type', 'Unknown')
                
                if item_type == 'worksheet':
                    chart_type = item_info.get('class', 'Unknown')
                    mark_type = item_info.get('mark_type', 'Unknown')
                    size = 'N/A'
                    used_fields = '; '.join(item_info.get('used_fields', []))
                    
                    # Format filters with type and field info
                    filters = []
                    filter_functions = []
                    filter_operations = []
                    filter_values = []
                    filter_descriptions = []
                    
                    for f in item_info.get('filters', []):
                        filter_desc = f"{f['field']}({f['type']})"
                        if f['name'] and f['name'] != f['field']:
                            filter_desc = f"{f['name']}: {filter_desc}"
                        filters.append(filter_desc)
                        
                        # Extract detailed filter information
                        filter_functions.append(f.get('function', ''))
                        filter_operations.append(f.get('operation', ''))
                        
                        # Format filter values
                        values = f.get('values', [])
                        if values:
                            filter_values.append('; '.join(values))
                        else:
                            filter_values.append('')
                        
                        filter_descriptions.append(f.get('description', ''))
                    
                    filters_str = '; '.join(filters)
                    filter_functions_str = '; '.join(filter_functions)
                    filter_operations_str = '; '.join(filter_operations)
                    filter_values_str = '; '.join(filter_values)
                    filter_descriptions_str = '; '.join(filter_descriptions)
                    
                    # Format slicers
                    slicers = '; '.join(item_info.get('slicers', []))
                    
                    # Format layout information
                    rows_layout = '; '.join(item_info.get('rows_layout', []))
                    columns_layout = '; '.join(item_info.get('columns_layout', []))
                    
                    # Format cards layout (UI structure)
                    cards_layout = []
                    for edge, cards in item_info.get('cards_layout', {}).items():
                        cards_layout.append(f"{edge}: {', '.join(cards)}")
                    cards_layout_str = '; '.join(cards_layout)
                    
                    # Aggregation setting
                    aggregation = 'Yes' if item_info.get('aggregation_enabled', False) else 'No'
                    
                    # Power BI recommendations based on chart type and layout
                    powerbi_recommendations = self.get_powerbi_chart_recommendation(chart_type, item_info)
                    
                elif item_type == 'dashboard':
                    chart_type = 'Dashboard'
                    mark_type = 'N/A'
                    width = item_info.get('width', 'Unknown')
                    height = item_info.get('height', 'Unknown')
                    size = f"{width}x{height}"
                    used_fields = 'N/A'
                    filters = '; '.join([f"{f['field']}({f['type']})" for f in item_info.get('filters', [])])
                    filter_functions_str = 'N/A'
                    filter_operations_str = 'N/A'
                    filter_values_str = 'N/A'
                    filter_descriptions_str = 'N/A'
                    slicers = 'N/A'
                    rows_layout = 'N/A'
                    columns_layout = 'N/A'
                    cards_layout_str = 'N/A'
                    aggregation = 'N/A'
                    powerbi_recommendations = 'Create new Power BI report page with same layout'
                
                else:
                    chart_type = 'Unknown'
                    mark_type = 'Unknown'
                    size = 'N/A'
                    used_fields = 'N/A'
                    filters = 'N/A'
                    filter_functions_str = 'N/A'
                    filter_operations_str = 'N/A'
                    filter_values_str = 'N/A'
                    filter_descriptions_str = 'N/A'
                    slicers = 'N/A'
                    rows_layout = 'N/A'
                    columns_layout = 'N/A'
                    cards_layout_str = 'N/A'
                    aggregation = 'N/A'
                    powerbi_recommendations = 'Review manually'
                
                writer.writerow({
                    'Item_Name': item_name,
                    'Item_Type': item_type,
                    'Chart_Type': chart_type,
                    'Mark_Type': mark_type,
                    'Size': size,
                    'Used_Fields': used_fields,
                    'Filters': filters_str if item_type == 'worksheet' else filters,
                    'Filter_Function': filter_functions_str,
                    'Filter_Operation': filter_operations_str,
                    'Filter_Values': filter_values_str,
                    'Filter_Description': filter_descriptions_str,
                    'Slicers': slicers,
                    'Rows_Layout': rows_layout,
                    'Columns_Layout': columns_layout,
                    'Cards_Layout': cards_layout_str,
                    'Aggregation': aggregation,
                    'Power_BI_Recommendations': powerbi_recommendations
                })

    def get_powerbi_chart_recommendation(self, tableau_chart_type, item_info=None):
        """Get Power BI chart recommendations based on Tableau chart type and layout."""
        chart_mappings = {
            'bar': 'Clustered Column Chart or Bar Chart',
            'line': 'Line Chart',
            'scatter': 'Scatter Chart',
            'crosstab': 'Matrix Visual',
            'map': 'Map Visual (with geographic field mapping)',
            'pie': 'Pie Chart or Donut Chart',
            'area': 'Area Chart',
            'heatmap': 'Matrix Visual with conditional formatting',
            'treemap': 'Treemap Visual',
            'bubble': 'Scatter Chart with size field',
            'histogram': 'Column Chart with binning',
            'box': 'Box and Whisker Chart',
            'gantt': 'Gantt Chart (custom visual)',
            'funnel': 'Funnel Chart',
            'bullet': 'Column Chart with target line'
        }
        
        # Clean the chart type and get recommendation
        clean_type = tableau_chart_type.lower().replace('_', '').replace('-', '')
        for key, recommendation in chart_mappings.items():
            if key in clean_type:
                return recommendation
        
        # If we have additional layout information, provide more specific recommendations
        if item_info and item_info.get('type') == 'worksheet':
            # Check if it's a table-like structure
            if item_info.get('class') == 'Table':
                if item_info.get('rows_layout') and item_info.get('columns_layout'):
                    return 'Matrix Visual with rows and columns layout'
                elif item_info.get('rows_layout'):
                    return 'Table Visual with row grouping'
                else:
                    return 'Table Visual'
            
            # Check mark type for additional context
            mark_type = item_info.get('mark_type', '')
            if mark_type == 'Automatic':
                return 'Auto-chart (Power BI will suggest best visual)'
            elif mark_type != 'Unknown':
                return f'Custom mark type: {mark_type} - review for Power BI equivalent'
        
        return 'Review chart type manually for Power BI equivalent'
