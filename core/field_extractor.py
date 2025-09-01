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
