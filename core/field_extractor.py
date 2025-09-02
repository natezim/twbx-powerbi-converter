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
