#!/usr/bin/env python3
"""
Field Extractor Module
Handles field metadata extraction and usage tracking
"""

import xml.etree.ElementTree as ET


class FieldExtractor:
    """Extracts field metadata and tracks usage in Tableau workbooks."""
    
    def __init__(self, xml_root, workbook):
        self.xml_root = xml_root
        self.workbook = workbook
    
    def extract_field_metadata(self, datasource_name):
        """Extract rich field metadata from XML including usage tracking."""
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
                        
                        # Parse the value to get table and field separately
                        if '.' in value:
                            table_name, field_name = value.split('.', 1)
                        else:
                            table_name = value
                            field_name = key
                        
                        field_metadata[key] = {
                            'table_reference': table_name,  # Just the table name, not table.field
                            'table_name': table_name,
                            'remote_name': field_name,  # The actual field name from database
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
                                    'table_reference': parent_table,  # Just the table name
                                    'table_name': parent_table,
                                    'used_in_workbook': False
                                }
                
                # Extract calculated fields from workbook XML
                self.extract_calculated_fields_from_workbook(field_metadata)
                
                # Now check for field usage across the entire workbook
                self.track_field_usage(field_metadata)
                break
        
        return field_metadata
    
    def track_field_usage(self, field_metadata):
        """Track which fields are actually used in the workbook using official Tableau API."""
        if not self.workbook:
            return
        
        # Use the official Tableau API to check field usage
        for datasource in self.workbook.datasources:
            # According to API docs, datasource.fields returns key-value pairs
            if hasattr(datasource, 'fields') and hasattr(datasource.fields, 'items'):
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

    def extract_calculated_fields_from_workbook(self, field_metadata):
        """Extract calculated field information from workbook XML."""
        if not self.xml_root:
            return
        
        print(f"🔍 Looking for calculated fields in workbook XML...")
        
        # Look for calculated fields in the workbook XML
        calculated_columns = self.xml_root.findall('.//column[calculation]')
        print(f"   Found {len(calculated_columns)} columns with calculations in workbook XML")
        
        for column in calculated_columns:
            # Get the actual field name from the XML attributes (like field.py does)
            column_name = column.get('name', '')
            if not column_name:
                continue
            
            # Clean the name (remove brackets) - this is the actual field name
            clean_name = column_name.replace('[', '').replace(']', '')
            
            # Get the caption (display name) if available
            caption = column.get('caption', clean_name)
            
            print(f"   Processing calculated field: {clean_name}")
            print(f"     Caption: {caption}")
            
            # Check if this column has a calculation
            calculation_elem = column.find('calculation')
            if calculation_elem is not None:
                formula = calculation_elem.get('formula', '')
                calc_class = calculation_elem.get('class', 'tableau')
                
                print(f"     Formula: {formula[:50]}{'...' if len(formula) > 50 else ''}")
                print(f"     Class: {calc_class}")
                
                # Check if this field already exists in metadata and update it
                if clean_name in field_metadata:
                    print(f"     Updating existing field: {clean_name}")
                    field_metadata[clean_name].update({
                        'is_calculated': True,
                        'calculation_formula': formula,
                        'calculation_class': calc_class,
                        'table_reference': None,  # Calculated fields don't have table references
                        'remote_name': None,  # Calculated fields don't have remote names
                        'used_in_workbook': True  # Mark calculated fields as used since they exist in the workbook
                    })
                else:
                    print(f"     Field {clean_name} not found in metadata, checking for partial matches...")
                    # Try to find partial matches in the metadata
                    found_match = False
                    for metadata_key in field_metadata.keys():
                        if clean_name in metadata_key or metadata_key in clean_name:
                            print(f"     Found partial match: {metadata_key} -> updating as calculated field")
                            field_metadata[metadata_key].update({
                                'is_calculated': True,
                                'calculation_formula': formula,
                                'calculation_class': calc_class,
                                'table_reference': None,
                                'remote_name': None,
                                'used_in_workbook': True
                            })
                            found_match = True
                            break
                    
                    if not found_match:
                        print(f"     Creating new calculated field with caption: {caption}")
                        # Create new calculated field entry using the caption (display name) as the key
                        field_metadata[caption] = {
                            'name': caption,  # Use caption as the field name (what users see in Tableau)
                            'caption': caption,  # This is the display name
                            'datatype': column.get('datatype', 'Unknown'),
                            'role': column.get('role', 'Unknown'),
                            'type': column.get('type', 'Unknown'),
                            'is_calculated': True,
                            'calculation_formula': formula,
                            'calculation_class': calc_class,
                            'table_reference': None,  # Calculated fields don't have table references
                            'remote_name': None,  # Calculated fields don't have remote names
                            'used_in_workbook': True,  # Mark calculated fields as used since they exist in the workbook
                            'data_type': column.get('datatype', 'Unknown'),
                            'parent_table': 'Workbook',  # Calculated fields are created in the workbook
                            'table_name': 'Workbook'  # Set table name to Workbook for calculated fields
                        }
        
        # Count calculated fields
        calc_count = sum(1 for field in field_metadata.values() if field.get('is_calculated', False))
        print(f"   Total calculated fields in metadata: {calc_count}")
        
        # Debug: Show some field names in metadata
        print(f"   Sample fields in metadata: {list(field_metadata.keys())[:5]}")
        if calc_count > 0:
            print(f"   Calculated field names: {[k for k, v in field_metadata.items() if v.get('is_calculated', False)]}")
