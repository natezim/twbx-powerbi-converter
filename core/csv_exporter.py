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
        os.makedirs(output_dir, exist_ok=True)
        
        for ds in data_sources:
            if not ds['fields']:
                continue
            
            # Create CSV filename
            safe_name = ds['caption'].replace(' ', '_').replace('/', '_')
            safe_name = ''.join(c for c in safe_name if c.isalnum() or c in '_-')
            csv_file = os.path.join(output_dir, f"{safe_name}_field_mapping.csv")
            
                            # Sort fields by table name first, then by column name
            # Put calculated fields at the end since they don't have table names
            sorted_fields = sorted(ds['fields'], key=lambda x: (
                x.get('is_calculated', False),  # Calculated fields last
                x.get('table_name', ''), 
                x.get('remote_name', '')
            ))
            
            # Debug: Show calculated fields
            calc_fields = [f for f in ds['fields'] if f.get('is_calculated', False)]
            print(f"   CSV Export: Found {len(calc_fields)} calculated fields: {[f.get('name', 'Unknown') for f in calc_fields]}")
            
            # Write CSV with field mapping including usage and calculated field info
            with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['Original_Field_Name', 'Tableau_Field_Name', 'Data_Type', 'Table_Name', 'Table_Reference_SQL', 'Used_In_Workbook', 'Is_Calculated', 'Calculation_Formula']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                # Write header
                writer.writeheader()
                
                # Write field mapping data in sorted order
                for field in sorted_fields:
                    # Clean up field names for better readability
                    original_name = field.get('remote_name', '') or ''
                    original_name = original_name.strip() if original_name else ''
                    tableau_name = field.get('name', '') or ''
                    tableau_name = tableau_name.strip() if tableau_name else ''
                    is_calculated = field.get('is_calculated', False)
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
                    if table_ref and original_name and not is_calculated:
                        if table_ref != tableau_name:
                            # Field was renamed in Tableau - add quotes around tableau_name if it has spaces
                            if ' ' in tableau_name:
                                table_ref_sql = f"{table_ref}.{original_name} as '{tableau_name}'"
                            else:
                                table_ref_sql = f"{table_ref}.{original_name} as {tableau_name}"
                        else:
                            # Field wasn't renamed, just use original
                            table_ref_sql = f"{table_ref}.{original_name}"
                    elif is_calculated:
                        # For calculated fields, show the actual Tableau field name
                        tableau_display_name = field.get('name', tableau_name)
                        table_ref_sql = f"CALCULATED: {tableau_display_name}"
                    else:
                        # Use table_name as fallback
                        table_name = field.get('table_name', 'Unknown') or 'Unknown'
                        table_ref_sql = f"{table_name}.{original_name}"
                    
                    # Clean up any special characters that might cause CSV issues
                    table_ref_sql = table_ref_sql.replace('"', '').replace("'", '').replace('\n', ' ').replace('\r', ' ')
                    
                    # Mark if field is used in the workbook
                    used_status = 'Yes' if field.get('used_in_workbook', False) else 'No'
                    
                    # Mark if field is calculated
                    calculated_status = 'Yes' if is_calculated else 'No'
                    
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
                        'Is_Calculated': calculated_status,
                        'Calculation_Formula': clean_formula
                    })
            
            print(f"✅ Created field mapping CSV: {csv_file}")

    def export_setup_guide_txt(self, output_dir, data_sources):
        """Export a simple text setup guide for Power BI migration."""
        for datasource_info in data_sources:
            # Create filename
            safe_name = create_safe_filename(datasource_info['caption'])
            txt_filename = f"{safe_name}_setup_guide.txt"
            txt_path = os.path.join(output_dir, txt_filename)
            
            with open(txt_path, 'w', encoding='utf-8') as f:
                f.write(f"POWER BI SETUP GUIDE\n")
                f.write(f"==================\n\n")
                f.write(f"Data Source: {datasource_info['name']}\n")
                f.write(f"Caption: {datasource_info.get('caption', 'N/A')}\n")
                f.write(f"Fields Available: {datasource_info.get('field_count', 0)}\n")
                
                # Count used fields and calculated fields
                used_fields = sum(1 for field in datasource_info.get('fields', []) if field.get('used_in_workbook', False))
                calculated_fields = sum(1 for field in datasource_info.get('fields', []) if field.get('is_calculated', False))
                f.write(f"Fields Used in Workbook: {used_fields}\n")
                f.write(f"Calculated Fields: {calculated_fields}\n\n")
                
                # Connection details
                if datasource_info.get('connections'):
                    f.write(f"CONNECTION DETAILS:\n")
                    f.write(f"------------------\n")
                    for i, conn in enumerate(datasource_info['connections'], 1):
                        f.write(f"Connection {i}:\n")
                        f.write(f"  Server: {conn.get('server', 'N/A')}\n")
                        f.write(f"  Database: {conn.get('dbname', 'N/A')}\n")
                        f.write(f"  Username: {conn.get('username', 'N/A')}\n")
                        f.write(f"  Type: {conn.get('dbclass', 'N/A')}\n")
                        f.write(f"  Port: {conn.get('port', 'N/A')}\n\n")
                
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
                
                # Join conditions with types
                if datasource_info.get('sql_info', {}).get('relationships') or datasource_info.get('sql_info', {}).get('join_conditions'):
                    f.write(f"CREATE THESE RELATIONSHIPS IN POWER BI MODEL VIEW:\n")
                    f.write(f"------------------------------------------------\n")
                    
                    # Collect unique relationships to avoid duplicates
                    unique_relationships = {}
                    
                    # Process main relationships
                    if datasource_info['sql_info'].get('relationships'):
                        for rel in datasource_info['sql_info']['relationships']:
                            join_type = rel.get('join_type', 'LEFT JOIN').upper()
                            
                            # Create a unique key for this relationship
                            conditions_key = '|'.join(sorted(rel.get('conditions', [])))
                            if conditions_key not in unique_relationships:
                                unique_relationships[conditions_key] = {
                                    'join_type': join_type,
                                    'conditions': rel.get('conditions', []),
                                    'tables': rel.get('tables', [])
                                }
                    
                    # Process additional join conditions
                    additional_joins = datasource_info['sql_info'].get('join_conditions', [])
                    for condition in additional_joins:
                        if condition not in [cond for rel in unique_relationships.values() for cond in rel['conditions']]:
                            # This is a truly additional condition
                            unique_relationships[f"additional_{condition}"] = {
                                'join_type': 'LEFT JOIN',
                                'conditions': [condition],
                                'tables': []
                            }
                    
                    # Display unique relationships in simple SQL-like format
                    if unique_relationships:
                        for i, (key, rel) in enumerate(unique_relationships.items(), 1):
                            join_type = rel['join_type']
                            
                            # Extract table names from conditions for simple display
                            if rel['conditions']:
                                # Get the first condition to show the basic relationship
                                first_condition = rel['conditions'][0]
                                if '=' in first_condition:
                                    left_part, right_part = first_condition.split('=', 1)
                                    left_table = left_part.split('.')[0].strip()
                                    right_table = right_part.split('.')[0].strip()
                                    
                                    # Convert aliases to actual table names
                                    actual_left_table = self.get_actual_table_name(left_table, datasource_info)
                                    actual_right_table = self.get_actual_table_name(right_table, datasource_info)
                                    
                                    # Extract field names from the condition
                                    left_field = left_part.split('.')[1].strip() if '.' in left_part else ''
                                    right_field = right_part.split('.')[1].strip() if '.' in right_part else ''
                                    
                                    # Get original Tableau aliases (with spaces) from the table mapping
                                    left_original_alias = self.get_original_alias(left_table, datasource_info)
                                    right_original_alias = self.get_original_alias(right_table, datasource_info)
                                    
                                    # Format the relationship with AS for both tables
                                    # Add quotes around aliases with spaces
                                    left_alias = f'"{left_original_alias}"' if ' ' in left_original_alias else left_original_alias
                                    right_alias = f'"{right_original_alias}"' if ' ' in right_original_alias else right_original_alias
                                    f.write(f"{i}. {join_type} JOIN {actual_left_table} AS {left_alias} ON {actual_left_table}.{left_field} = {actual_right_table} AS {right_alias}.{right_field}\n")
                                else:
                                    f.write(f"{i}. {join_type} relationship: {first_condition}\n")
                            else:
                                f.write(f"{i}. {join_type} relationship\n")
                    
                                    # No fluff - just the relationships
                if not unique_relationships:
                    f.write(f"No relationships found\n")
                
                # SQL-ready column list for used fields (skip calculated fields)
                used_fields = [field for field in datasource_info.get('fields', []) 
                             if field.get('used_in_workbook', False) and 
                             field.get('table_reference') and  # Must have a table reference (not calculated)
                             field.get('remote_name')]  # Must have an original field name
                
                # Calculated fields section
                calculated_fields = [field for field in datasource_info.get('fields', []) 
                                   if field.get('is_calculated', False) and 
                                   field.get('used_in_workbook', False)]
                
                if calculated_fields:
                    f.write(f"\n")
                    f.write(f"CALCULATED FIELDS:\n")
                    f.write(f"------------------\n")
                    
                    # Sort calculated fields by name
                    sorted_calc_fields = sorted(calculated_fields, key=lambda x: x.get('name', ''))
                    
                    for field in sorted_calc_fields:
                        field_name = field.get('name', '').strip()
                        formula = field.get('calculation_formula', '').strip()
                        data_type = field.get('data_type', 'Unknown')
                        role = field.get('role', 'Unknown')
                        
                        f.write(f"{field_name} ({data_type}, {role}):\n")
                        f.write(f"  {formula}\n\n")
                
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
