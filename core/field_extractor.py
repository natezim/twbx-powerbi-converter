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
    
    def clean_field_name(self, field_name):
        """Clean field name by removing numbers at the beginning and extra spaces."""
        import re
        
        # Remove numbers and dots at the beginning (e.g., "1. Period" -> "Period")
        cleaned = re.sub(r'^\d+\.?\s*', '', field_name)
        
        # Remove extra spaces
        cleaned = cleaned.strip()
        
        return cleaned
    
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
                            
                            # Get aggregation and role separately
                            aggregation = record.find('aggregation')
                            aggregation_text = aggregation.text if aggregation is not None else 'None'
                            
                            # Determine role based on aggregation type
                            if aggregation_text in ['Sum', 'Count', 'Average', 'Min', 'Max']:
                                role = 'measure'
                            elif aggregation_text == 'None':
                                role = 'dimension'
                            else:
                                role = 'dimension'  # Default to dimension for other cases
                            
                            # Get parent table
                            parent_name = record.find('parent-name')
                            parent_table = str(parent_name.text).replace('[', '').replace(']', '') if parent_name is not None and parent_name.text is not None else 'Unknown'
                            
                            # Get remote name (original database field)
                            remote_name = record.find('remote-name')
                            remote_field = str(remote_name.text) if remote_name is not None and remote_name.text is not None else field_name
                            
                            # Update field metadata with rich information
                            if field_name in field_metadata:
                                field_metadata[field_name].update({
                                    'data_type': data_type,
                                    'role': role,
                                    'aggregation': aggregation_text,
                                    'parent_table': parent_table,
                                    'remote_name': remote_field
                                })
                            else:
                                field_metadata[field_name] = {
                                    'data_type': data_type,
                                    'role': role,
                                    'aggregation': aggregation_text,
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

    def extract_filter_details(self, filter_elem):
        """Extract detailed filter information including function, operation, and values."""
        filter_details = {}
        
        # Get the main groupfilter
        main_groupfilter = filter_elem.find('.//groupfilter')
        if main_groupfilter is not None:
            # Extract filter function (union, except, etc.)
            filter_details['function'] = main_groupfilter.get('function', '')
            
            # Extract operation details
            filter_details['operation'] = main_groupfilter.get('user:op', '')
            
            # Extract behavior (exclusive, relevant, etc.)
            filter_details['behavior'] = main_groupfilter.get('user:ui-domain', '')
            
            # Extract filter values/members
            values = []
            for member_filter in main_groupfilter.findall('.//groupfilter[@function="member"]'):
                member_value = member_filter.get('member', '')
                if member_value:
                    # Clean up the member value
                    clean_value = member_value.replace('&quot;', '"').replace('[', '').replace(']', '')
                    # Extract just the field name part if it's a complex reference
                    if '.' in clean_value:
                        field_part = clean_value.split('.')[-1]
                    else:
                        field_part = clean_value
                    values.append(field_part)
            
            filter_details['values'] = values
            
            # Create a human-readable description
            if filter_details['function'] == 'union':
                if values:
                    filter_details['description'] = f"Show only: {', '.join(values)}"
                else:
                    filter_details['description'] = "Include specific values"
            elif filter_details['function'] == 'except':
                if values:
                    filter_details['description'] = f"Exclude: {', '.join(values)}"
                else:
                    filter_details['description'] = "Exclude specific values"
            elif filter_details['function'] == 'level-members':
                filter_details['description'] = "Show all values in level"
            else:
                filter_details['description'] = f"{filter_details['function']} operation"
        
        return filter_details

    def extract_dashboard_worksheet_info(self, xml_root):
        """Extract dashboard and worksheet information with field usage and filters."""
        if not self.xml_root:
            return {}
        
        print(f"🔍 Extracting dashboard and worksheet information...")
        
        dashboard_info = {}
        
        # Extract worksheets
        worksheets = self.xml_root.findall('.//worksheet')
        print(f"   Found {len(worksheets)} worksheets")
        
        for worksheet in worksheets:
            worksheet_name = worksheet.get('name', 'Unknown')
            print(f"   Processing worksheet: {worksheet_name}")
            
            # Get worksheet type (chart type) - look for table, chart, map, etc.
            worksheet_type = 'Unknown'
            table_elem = worksheet.find('.//table')
            chart_elem = worksheet.find('.//chart')
            map_elem = worksheet.find('.//map')
            
            if table_elem is not None:
                worksheet_type = 'Table'
            elif chart_elem is not None:
                worksheet_type = 'Chart'
            elif map_elem is not None:
                worksheet_type = 'Map'
            
            # Extract fields used in this worksheet
            used_fields = []
            field_elements = worksheet.findall('.//column')
            for field_elem in field_elements:
                field_name = field_elem.get('name', '')
                if field_name:
                    # For calculated fields, use the caption (display name) instead of internal ID
                    if field_elem.find('calculation') is not None:
                        # This is a calculated field - use caption if available
                        caption = field_elem.get('caption', '')
                        if caption:
                            used_fields.append(caption)
                        else:
                            # Fallback to cleaned name if no caption
                            clean_name = field_name.replace('[', '').replace(']', '')
                            used_fields.append(clean_name)
                    else:
                        # Regular field - use cleaned name
                        clean_name = field_name.replace('[', '').replace(']', '')
                        used_fields.append(clean_name)
            
            # Extract filters applied to this worksheet
            filters = []
            filter_elements = worksheet.findall('.//filter')
            for filter_elem in filter_elements:
                filter_name = filter_elem.get('name', '')
                filter_type = filter_elem.get('class', 'Unknown')
                filter_field = filter_elem.get('column', '')
                
                if filter_field:
                    # Clean up the filter field name
                    clean_filter_field = filter_field.replace('[', '').replace(']', '')
                    # Extract just the field name part (after the last dot)
                    if '.' in clean_filter_field:
                        field_part = clean_filter_field.split('.')[-1]
                    else:
                        field_part = clean_filter_field
                    
                    # Extract detailed filter information
                    filter_details = self.extract_filter_details(filter_elem)
                    
                    filters.append({
                        'name': filter_name or field_part,
                        'type': filter_type,
                        'field': field_part,
                        'full_column': clean_filter_field,
                        'function': filter_details.get('function', ''),
                        'operation': filter_details.get('operation', ''),
                        'values': filter_details.get('values', []),
                        'behavior': filter_details.get('behavior', ''),
                        'description': filter_details.get('description', '')
                    })
            
            # Extract slicers (what users can filter on)
            slicers = []
            slice_elements = worksheet.findall('.//slices/column')
            for slice_elem in slice_elements:
                slice_text = slice_elem.text
                if slice_text:
                    clean_slice = slice_text.replace('[', '').replace(']', '')
                    # Extract just the field name part
                    if '.' in clean_slice:
                        field_part = clean_slice.split('.')[-1]
                    else:
                        field_part = clean_slice
                    slicers.append(field_part)
            
            # Extract layout information (rows and columns)
            rows_layout = []
            cols_layout = []
            
            rows_elem = worksheet.find('.//rows')
            if rows_elem is not None and rows_elem.text:
                rows_text = rows_elem.text
                # Parse the complex row layout (e.g., "field1 / field2 / field3")
                if '/' in rows_text:
                    row_fields = [f.strip().replace('[', '').replace(']', '') for f in rows_text.split('/')]
                    rows_layout = [f for f in row_fields if f and not f.startswith('(') and not f.endswith(')')]
                else:
                    clean_row = rows_text.replace('[', '').replace(']', '')
                    if clean_row:
                        rows_layout.append(clean_row)
            
            cols_elem = worksheet.find('.//cols')
            if cols_elem is not None and cols_elem.text:
                cols_text = cols_elem.text
                if cols_text:
                    clean_col = cols_text.replace('[', '').replace(']', '')
                    if clean_col:
                        cols_layout.append(clean_col)
            
            # Extract card layout information (UI structure)
            cards_info = {}
            cards_elem = worksheet.find('.//cards')
            if cards_elem is not None:
                for edge in cards_elem.findall('.//edge'):
                    edge_name = edge.get('name', '')
                    edge_cards = []
                    for card in edge.findall('.//card'):
                        card_type = card.get('type', '')
                        edge_cards.append(card_type)
                    if edge_cards:
                        cards_info[edge_name] = edge_cards
            
            # Extract mark type (what kind of marks are used)
            mark_type = 'Unknown'
            mark_elem = worksheet.find('.//mark')
            if mark_elem is not None:
                mark_type = mark_elem.get('class', 'Unknown')
            
            # Extract aggregation settings
            aggregation_enabled = False
            agg_elem = worksheet.find('.//aggregation')
            if agg_elem is not None:
                aggregation_enabled = agg_elem.get('value', 'false') == 'true'
            
            dashboard_info[worksheet_name] = {
                'type': 'worksheet',
                'class': worksheet_type,
                'mark_type': mark_type,
                'aggregation_enabled': aggregation_enabled,
                'used_fields': used_fields,
                'filters': filters,
                'slicers': slicers,
                'rows_layout': rows_layout,
                'columns_layout': cols_layout,
                'cards_layout': cards_info
            }
        
        # Extract dashboards
        dashboards = self.xml_root.findall('.//dashboard')
        print(f"   Found {len(dashboards)} dashboards")
        
        for dashboard in dashboards:
            dashboard_name = dashboard.get('name', 'Unknown')
            print(f"   Processing dashboard: {dashboard_name}")
            
            # Get dashboard size
            size_elem = dashboard.find('.//size')
            width = size_elem.get('width', 'Unknown') if size_elem is not None else 'Unknown'
            height = size_elem.get('height', 'Unknown') if size_elem is not None else 'Unknown'
            
            # Extract worksheets included in this dashboard
            included_worksheets = []
            worksheet_elements = dashboard.findall('.//worksheet')
            for ws_elem in worksheet_elements:
                ws_name = ws_elem.get('name', '')
                if ws_name:
                    included_worksheets.append(ws_name)
            
            # Extract dashboard-level filters
            dashboard_filters = []
            filter_elements = dashboard.findall('.//filter')
            for filter_elem in filter_elements:
                filter_name = filter_elem.get('name', '')
                filter_type = filter_elem.get('class', 'Unknown')
                filter_field = filter_elem.get('field', '')
                
                if filter_name:
                    dashboard_filters.append({
                        'name': filter_name,
                        'type': filter_type,
                        'field': filter_field.replace('[', '').replace(']', '') if filter_field else ''
                    })
            
            dashboard_info[dashboard_name] = {
                'type': 'dashboard',
                'width': width,
                'height': height,
                'included_worksheets': included_worksheets,
                'filters': dashboard_filters
            }
        
        print(f"   Total items found: {len(dashboard_info)}")
        return dashboard_info

    def extract_calculated_fields_from_workbook(self, field_metadata):
        """Extract calculated field information from workbook XML using official Tableau API."""
        if not self.xml_root:
            return
        
        print(f"🔍 Looking for calculated fields and parameters in workbook XML...")
        
        # Use the official Tableau API to properly distinguish between calculated fields and parameters
        if not self.workbook:
            print("   Warning: No workbook object available, falling back to XML parsing")
            return
        
        # Process each datasource in the workbook
        for datasource in self.workbook.datasources:
            print(f"   Processing datasource: {datasource.name}")
            
            # Get all fields from this datasource using the official API
            if hasattr(datasource, 'fields'):
                for field_name, field_obj in datasource.fields.items():
                    # Clean the field name (remove brackets)
                    clean_name = field_name.replace('[', '').replace(']', '')
                    
                    # Get the caption (display name) - this is what users see in Tableau
                    caption = getattr(field_obj, 'caption', clean_name)
                    if not caption:
                        caption = clean_name
                    
                    # Clean the caption to remove numbers at the beginning
                    clean_caption = self.clean_field_name(caption)
                    
                    print(f"     Processing field: {clean_name} -> {clean_caption}")
                    print(f"       Caption: {caption}")
                    
                    # Check if this is a calculated field using the official API
                    if hasattr(field_obj, 'calculation') and field_obj.calculation:
                        print(f"       Type: Calculated Field")
                        print(f"       Formula: {field_obj.calculation[:50]}{'...' if len(field_obj.calculation) > 50 else ''}")
                        
                        # Create or update calculated field entry
                        if clean_caption in field_metadata:
                            field_metadata[clean_caption].update({
                                'is_calculated': True,
                                'is_parameter': False,
                                'calculation_formula': field_obj.calculation,
                                'calculation_class': 'tableau',
                                'datatype': getattr(field_obj, 'datatype', 'Unknown'),
                                'role': getattr(field_obj, 'role', 'Unknown'),
                                'type': getattr(field_obj, 'type', 'Unknown'),
                                'used_in_workbook': True
                            })
                        else:
                            field_metadata[clean_caption] = {
                                'name': clean_caption,
                                'caption': caption,
                                'datatype': getattr(field_obj, 'datatype', 'Unknown'),
                                'role': getattr(field_obj, 'role', 'Unknown'),
                                'type': getattr(field_obj, 'type', 'Unknown'),
                                'is_calculated': True,
                                'is_parameter': False,
                                'field_type': 'Calculated',
                                'calculation_formula': field_obj.calculation,
                                'calculation_class': 'tableau',
                                'table_reference': None,
                                'remote_name': None,
                                'used_in_workbook': True,
                                'data_type': getattr(field_obj, 'datatype', 'Unknown'),
                                'parent_table': 'Workbook',
                                'table_name': 'Workbook'
                            }
                    
                    # Check if this is a parameter field
                    # Parameters in Tableau have specific attributes in the XML
                    elif hasattr(field_obj, 'xml') and field_obj.xml is not None:
                        # Check if the XML element has param-domain-type attribute (indicates parameter)
                        if field_obj.xml.get('param-domain-type') is not None:
                            print(f"       Type: Parameter")
                            
                            # Get formula if available
                            formula = ''
                            if hasattr(field_obj, 'calculation') and field_obj.calculation:
                                formula = field_obj.calculation
                                print(f"       Formula: {formula[:50]}{'...' if len(formula) > 50 else ''}")
                            
                            # Create or update parameter field entry
                            if clean_caption in field_metadata:
                                field_metadata[clean_caption].update({
                                    'is_calculated': False,
                                    'is_parameter': True,
                                    'calculation_formula': formula,
                                    'calculation_class': 'tableau',
                                    'datatype': getattr(field_obj, 'datatype', 'Unknown'),
                                    'role': getattr(field_obj, 'role', 'Unknown'),
                                    'type': getattr(field_obj, 'type', 'Unknown'),
                                    'used_in_workbook': True
                                })
                            else:
                                field_metadata[clean_caption] = {
                                    'name': clean_caption,
                                    'caption': caption,
                                    'datatype': getattr(field_obj, 'datatype', 'Unknown'),
                                    'role': getattr(field_obj, 'role', 'Unknown'),
                                    'type': getattr(field_obj, 'type', 'Unknown'),
                                    'is_calculated': False,
                                    'is_parameter': True,
                                    'field_type': 'Parameter',
                                    'calculation_formula': formula,
                                    'calculation_class': 'tableau',
                                    'table_reference': None,
                                    'remote_name': None,
                                    'used_in_workbook': True,
                                    'data_type': getattr(field_obj, 'datatype', 'Unknown'),
                                    'parent_table': 'Workbook',
                                    'table_name': 'Workbook'
                                }
                        else:
                            print(f"       Type: Regular Field")
                    
                    # FALLBACK: If the official API didn't detect parameters, check XML directly
                    # This is needed because some Tableau versions don't expose param-domain-type properly
                    if not field_metadata.get(clean_caption, {}).get('is_parameter', False):
                        # Check if this field name matches a parameter pattern in the XML
                        if self.xml_root:
                            # Look for columns with param-domain-type attribute
                            # Use a simpler approach to avoid XPath syntax issues with brackets
                            param_elements = []
                            for col in self.xml_root.findall('.//column[@param-domain-type]'):
                                if col.get('name') == field_name:
                                    param_elements.append(col)
                                    break
                            
                            if param_elements:
                                print(f"       Type: Parameter (detected via XML fallback)")
                                
                                # Get the parameter element
                                param_elem = param_elements[0]
                                param_type = param_elem.get('param-domain-type', 'Unknown')
                                param_value = param_elem.get('value', '')
                                
                                # Try to get parameter value from calculation if available
                                if not param_value and hasattr(field_obj, 'calculation') and field_obj.calculation:
                                    param_value = field_obj.calculation
                                
                                # Create or update parameter field entry
                                if clean_caption in field_metadata:
                                    field_metadata[clean_caption].update({
                                        'is_calculated': False,
                                        'is_parameter': True,
                                        'calculation_formula': param_value,
                                        'calculation_class': 'tableau',
                                        'datatype': getattr(field_obj, 'datatype', 'Unknown'),
                                        'role': getattr(field_obj, 'role', 'Unknown'),
                                        'type': getattr(field_obj, 'type', 'Unknown'),
                                        'used_in_workbook': True
                                    })
                                else:
                                    field_metadata[clean_caption] = {
                                        'name': clean_caption,
                                        'caption': caption,
                                        'datatype': getattr(field_obj, 'datatype', 'Unknown'),
                                        'role': getattr(field_obj, 'role', 'Unknown'),
                                        'type': getattr(field_obj, 'type', 'Unknown'),
                                        'is_calculated': False,
                                        'is_parameter': True,
                                        'field_type': 'Parameter',
                                        'calculation_formula': param_value,
                                        'calculation_class': 'tableau',
                                        'table_reference': None,
                                        'remote_name': None,
                                        'used_in_workbook': True,
                                        'data_type': getattr(field_obj, 'datatype', 'Unknown'),
                                        'parent_table': 'Workbook',
                                        'table_name': 'Workbook'
                                    }
        
        # Count calculated fields and parameters
        calc_count = sum(1 for field in field_metadata.values() if field.get('is_calculated', False))
        param_count = sum(1 for field in field_metadata.values() if field.get('is_parameter', False))
        print(f"   Total calculated fields in metadata: {calc_count}")
        print(f"   Total parameters in metadata: {param_count}")
        
        # Debug: Show some field names in metadata
        print(f"   Sample fields in metadata: {list(field_metadata.keys())[:5]}")
        if calc_count > 0:
            print(f"   Calculated field names: {[k for k, v in field_metadata.items() if v.get('is_calculated', False)]}")
        if param_count > 0:
            print(f"   Parameter names: {[k for k, v in field_metadata.items() if v.get('is_parameter', False)]}")
        
        # Now resolve any calculation references to use friendly names
        self.resolve_calculation_references(field_metadata)

    def resolve_calculation_references(self, field_metadata):
        """Replace internal calculation IDs with friendly field names in formulas."""
        import re
        
        print("🔧 Resolving calculation references...")
        
        # FIRST PASS: Build a complete mapping from calculation IDs to friendly names
        calc_id_to_name = {}
        
        if self.xml_root:
            # Find all columns with calculations and build the mapping
            calculated_columns = self.xml_root.findall('.//column[calculation]')
            print(f"   Found {len(calculated_columns)} calculated columns in XML")
            
            for column in calculated_columns:
                column_name = column.get('name', '').replace('[', '').replace(']', '')
                caption = column.get('caption', column_name)
                
                # Map the calculation ID to its friendly name
                calc_id_to_name[column_name] = caption
                print(f"   Mapped calculation ID: {column_name} -> {caption}")
        
        # Also check our field metadata for any calculated fields we might have missed
        for field_name, field_info in field_metadata.items():
            if field_info.get('is_calculated', False):
                # If this field has a calculation ID pattern, map it to its friendly name
                if 'Calculation_' in field_name:
                    calc_id_to_name[field_name] = field_info.get('name', field_name)
                    print(f"   Mapped from metadata: {field_name} -> {field_info.get('name', field_name)}")
        
        print(f"   Total calculation mappings: {len(calc_id_to_name)}")
        
        # SECOND PASS: Replace calculation references in all formulas
        replaced_count = 0
        for field_name, field_info in field_metadata.items():
            if field_info.get('is_calculated', False):
                formula = field_info.get('calculation_formula', '')
                original_formula = formula
                
                # Replace all known calculation IDs with friendly names
                for calc_id, friendly_name in calc_id_to_name.items():
                    if f'[{calc_id}]' in formula:
                        # Clean the friendly name to remove numbers at the beginning
                        clean_friendly_name = self.clean_field_name(friendly_name)
                        formula = formula.replace(f'[{calc_id}]', f'[{clean_friendly_name}]')
                        print(f"   In '{field_name}': Replaced [{calc_id}] with [{clean_friendly_name}]")
                        replaced_count += 1
                
                # Also handle any remaining Calculation_XXXXXXXX patterns
                calc_pattern = re.compile(r'\[Calculation_(\d+)\]')
                matches = calc_pattern.findall(formula)
                
                for calc_num in matches:
                    calc_ref = f'Calculation_{calc_num}'
                    # Try to find a friendly name for this calculation ID
                    if calc_ref in calc_id_to_name:
                        # Clean the friendly name to remove numbers at the beginning
                        clean_friendly_name = self.clean_field_name(calc_id_to_name[calc_ref])
                        formula = formula.replace(f'[{calc_ref}]', f'[{clean_friendly_name}]')
                        print(f"   In '{field_name}': Replaced [{calc_ref}] with [{clean_friendly_name}]")
                        replaced_count += 1
                
                # Update the formula if it changed
                if formula != original_formula:
                    field_info['calculation_formula'] = formula
        
        print(f"   Total calculation references resolved: {replaced_count}")

    def extract_data_from_hyper_files(self, twbx_path):
        """Extract actual data from any .hyper files found in the TWBX."""
        try:
            from tableauhyperapi import HyperProcess, Connection, CreateMode, Telemetry
            import pandas as pd
            from pathlib import Path
            import os
        except ImportError as e:
            print(f"⚠️ Tableau Hyper API not available: {e}")
            print("   Install with: pip install tableauhyperapi")
            # Return a special marker to indicate missing dependency
            return {"__missing_dependency__": "tableauhyperapi"}
        
        extracted_data = {}
        
        # Create a temporary directory to extract TWBX contents
        import tempfile
        import zipfile
        
        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                # Extract TWBX file (it's a ZIP file)
                with zipfile.ZipFile(twbx_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
                
                # Look for .hyper files in the extracted TWBX
                hyper_files = list(Path(temp_dir).glob('**/*.hyper'))
                
                if not hyper_files:
                    print("   No .hyper files found in TWBX")
                    return {}
                
                print(f"   Found {len(hyper_files)} .hyper files")
                
                for hyper_file in hyper_files:
                    try:
                        print(f"     Processing: {hyper_file.name}")
                        
                        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
                            with Connection(hyper.endpoint, str(hyper_file), CreateMode.NONE) as connection:
                                
                                # Get all tables in the hyper file
                                table_names = connection.catalog.get_table_names('Extract')
                                
                                if not table_names:
                                    print(f"       No tables found in {hyper_file.name}")
                                    continue
                                
                                print(f"       Found {len(table_names)} tables")
                                
                                for table_name in table_names:
                                    try:
                                        print(f"         Processing table: {table_name}")
                                        
                                        # Get column schema first
                                        schema_query = f'SELECT * FROM {table_name} LIMIT 1'
                                        with connection.execute_query(schema_query) as schema_result:
                                            # Clean column names by removing quotes and extra characters
                                            columns = [str(col.name).replace('"', '').replace("'", "").strip() for col in schema_result.schema.columns]
                                        
                                        # Extract data using SQL
                                        query = f'SELECT * FROM {table_name}'
                                        with connection.execute_query(query) as result:
                                            # Convert result to list of tuples
                                            data = [tuple(row) for row in result]
                                        
                                        if not data:
                                            print(f"         Table {table_name}: No data")
                                            continue
                                        
                                        # Convert to pandas DataFrame
                                        df = pd.DataFrame(data, columns=columns)
                                        
                                        # Create a clean table name for the filename
                                        # Handle schema-qualified table names like "Extract"."Orders_ECFCA1FB690A41FE803BC071773BA862"
                                        clean_table_name = str(table_name)
                                        
                                        # Remove quotes and extract just the table part after the last dot
                                        clean_table_name = clean_table_name.replace('"', '').replace("'", "")
                                        
                                        # If it has a schema prefix (like "Extract"."TableName"), extract just the table part
                                        if '.' in clean_table_name:
                                            # Split by dot and take the last part (the actual table name)
                                            table_parts = clean_table_name.split('.')
                                            if len(table_parts) > 1:
                                                # Remove the schema part and get just the table name
                                                clean_table_name = table_parts[-1]
                                        
                                        # Remove common prefixes that Tableau adds to table names
                                        prefixes_to_remove = ['Extract', 'Sample_', 'Sample - ']
                                        for prefix in prefixes_to_remove:
                                            if clean_table_name.startswith(prefix):
                                                clean_table_name = clean_table_name[len(prefix):].strip()
                                                break
                                        
                                        # Remove the hash part (everything after the last underscore that's followed by a long hex string)
                                        # Pattern: TableName_Hash where Hash is a long hex string
                                        if '_' in clean_table_name:
                                            parts = clean_table_name.split('_')
                                            # Check if the last part looks like a hash (long hex string)
                                            if len(parts) > 1 and len(parts[-1]) > 20 and all(c in '0123456789ABCDEFabcdef' for c in parts[-1]):
                                                # Remove the hash part, keep the meaningful table name
                                                clean_table_name = '_'.join(parts[:-1])
                                        
                                        # Clean up special characters and make it safe for filenames
                                        clean_table_name = clean_table_name.replace('\\', '_').replace('/', '_').replace(' ', '_')
                                        # Remove any remaining special characters that aren't alphanumeric or underscore
                                        clean_table_name = ''.join(c for c in clean_table_name if c.isalnum() or c == '_')
                                        
                                        # Use just the clean table name as the key
                                        safe_key = clean_table_name
                                        
                                        extracted_data[safe_key] = {
                                            'dataframe': df,
                                            'row_count': len(df),
                                            'column_count': len(columns),
                                            'columns': columns,
                                            'source_file': hyper_file.name,
                                            'table_name': table_name,
                                            'safe_key': safe_key
                                        }
                                        
                                        print(f"         ✅ Extracted {len(df)} rows, {len(columns)} columns from {table_name}")
                                        
                                    except Exception as e:
                                        print(f"         ⚠️ Error extracting table {table_name}: {e}")
                                        continue
                                        
                    except Exception as e:
                        print(f"     ⚠️ Could not process {hyper_file.name}: {e}")
                        continue
                
                print(f"   Successfully extracted data from {len(extracted_data)} tables")
                
            except Exception as e:
                print(f"   ⚠️ Error extracting TWBX contents: {e}")
                return {}
        
        return extracted_data

    def export_hyper_data_to_excel(self, extracted_data, output_dir):
        """Export extracted hyper data to Excel files."""
        if not extracted_data:
            print("   No hyper data to export")
            return []
        
        try:
            import pandas as pd
            import os
        except ImportError as e:
            print(f"⚠️ Pandas not available: {e}")
            return []
        
        exported_files = []
        
        for table_key, table_info in extracted_data.items():
            try:
                df = table_info['dataframe']
                # Use just the clean table name for the filename
                safe_filename = f"{table_key}.xlsx"
                excel_path = os.path.join(output_dir, safe_filename)
                
                # Export to Excel
                df.to_excel(excel_path, index=False, engine='openpyxl')
                exported_files.append(excel_path)
                
                print(f"     📊 Saved {table_info['row_count']} rows to: {safe_filename}")
                
            except Exception as e:
                print(f"     ⚠️ Error exporting {table_key}: {e}")
                continue
        
        if exported_files:
            print(f"   ✅ Exported {len(exported_files)} Excel files")
        else:
            print("   ⚠️ No files were exported successfully")
        
        return exported_files
