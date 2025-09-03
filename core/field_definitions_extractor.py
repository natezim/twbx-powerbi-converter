"""
Universal Field Definitions Extractor

This module extracts comprehensive field definitions for troubleshooting and development purposes.
It captures ALL available field metadata from both the Tableau Document API and raw XML parsing.
"""

import xml.etree.ElementTree as ET
from typing import Dict, List, Any, Optional
from datetime import datetime


class FieldDefinitionsExtractor:
    """Extracts comprehensive field definitions for universal troubleshooting."""
    
    def __init__(self):
        self.field_definitions = {
            "extraction_metadata": {
                "extracted_at": datetime.now().isoformat(),
                "extractor_version": "1.0.0",
                "purpose": "Universal field definitions for troubleshooting and development"
            },
            "field_categories": {
                "regular_fields": [],
                "calculated_fields": [],
                "parameters": [],
                "table_fields": [],
                "internal_fields": []
            },
            "field_usage_tracking": {},
            "data_source_relationships": {},
            "field_dependencies": {}
        }
    
    def extract_all_field_definitions(self, workbook, xml_root: ET.Element) -> Dict[str, Any]:
        """Extract comprehensive field definitions from workbook and XML."""
        try:
            # Extract from Document API
            self._extract_from_document_api(workbook)
            
            # Extract from XML for additional details
            self._extract_from_xml(xml_root)
            
            # Analyze field relationships and usage
            self._analyze_field_relationships()
            
            return self.field_definitions
            
        except Exception as e:
            print(f"Error extracting field definitions: {e}")
            return self.field_definitions
    
    def _extract_from_document_api(self, workbook):
        """Extract field definitions using the Tableau Document API."""
        try:
            for datasource in workbook.datasources:
                datasource_name = datasource.name
                
                # Track data source relationships
                if datasource_name not in self.field_definitions["data_source_relationships"]:
                    self.field_definitions["data_source_relationships"][datasource_name] = {
                        "connection_info": {},
                        "field_count": 0,
                        "field_types": {}
                    }
                
                # Extract connection information
                for connection in datasource.connections:
                    conn_info = {
                        "server": getattr(connection, 'server', ''),
                        "dbname": getattr(connection, 'dbname', ''),
                        "username": getattr(connection, 'username', ''),
                        "authentication": getattr(connection, 'authentication', ''),
                        "class": getattr(connection, 'dbclass', ''),
                        "port": getattr(connection, 'port', ''),
                        "schema": getattr(connection, 'schema', ''),
                        "table": getattr(connection, 'table', ''),
                        "query": getattr(connection, 'query', '')
                    }
                    self.field_definitions["data_source_relationships"][datasource_name]["connection_info"] = conn_info
                
                # Extract all fields from this datasource using the proper API
                for field_name, field in datasource.fields.items():
                    field_def = self._extract_field_definition_from_api(field, datasource_name)
                    
                    # Categorize field using proper API methods
                    if self._is_parameter_from_api(field):
                        self.field_definitions["field_categories"]["parameters"].append(field_def)
                    elif self._is_calculated_field_from_api(field):
                        self.field_definitions["field_categories"]["calculated_fields"].append(field_def)
                    elif self._is_table_field_from_api(field):
                        self.field_definitions["field_categories"]["table_fields"].append(field_def)
                    elif self._is_internal_field_from_api(field):
                        self.field_definitions["field_categories"]["internal_fields"].append(field_def)
                    else:
                        self.field_definitions["field_categories"]["regular_fields"].append(field_def)
                    
                    # Update data source tracking
                    self.field_definitions["data_source_relationships"][datasource_name]["field_count"] += 1
                    field_type = field.datatype if field.datatype else 'unknown'
                    if field_type not in self.field_definitions["data_source_relationships"][datasource_name]["field_types"]:
                        self.field_definitions["data_source_relationships"][datasource_name]["field_types"][field_type] = 0
                    self.field_definitions["data_source_relationships"][datasource_name]["field_types"][field_type] += 1
                    
        except Exception as e:
            print(f"Error extracting from Document API: {e}")
            import traceback
            traceback.print_exc()
    
    def _extract_field_definition_from_api(self, field, datasource_name: str) -> Dict[str, Any]:
        """Extract comprehensive field definition from Document API field object."""
        field_def = {
            "name": field.name if hasattr(field, 'name') else 'Unknown',
            "id": field.id if hasattr(field, 'id') else 'Unknown',
            "caption": field.caption if hasattr(field, 'caption') else '',
            "alias": field.alias if hasattr(field, 'alias') else '',
            "datatype": field.datatype if hasattr(field, 'datatype') else 'unknown',
            "role": field.role if hasattr(field, 'role') else 'unknown',
            "type": field.type if hasattr(field, 'type') else 'unknown',
            "hidden": field.hidden if hasattr(field, 'hidden') else 'false',
            "calculation": field.calculation if hasattr(field, 'calculation') else None,
            "description": field.description if hasattr(field, 'description') else '',
            "default_aggregation": field.default_aggregation if hasattr(field, 'default_aggregation') else None,
            "datasource": datasource_name,
            "api_attributes": {},
            "xml_attributes": {},
            "usage_info": {
                "used_in_worksheets": field.worksheets if hasattr(field, 'worksheets') else [],
                "used_in_dashboards": [],
                "filter_usage": [],
                "calculation_dependencies": []
            }
        }
        
        # Capture additional API attributes
        api_attrs = {
            'is_quantitative': field.is_quantitative if hasattr(field, 'is_quantitative') else False,
            'is_ordinal': field.is_ordinal if hasattr(field, 'is_ordinal') else False,
            'is_nominal': field.is_nominal if hasattr(field, 'is_nominal') else False,
            'aliases': field.aliases if hasattr(field, 'aliases') else {}
        }
        field_def["api_attributes"] = api_attrs
        
        return field_def
    
    def _extract_from_xml(self, xml_root: ET.Element):
        """Extract additional field details from XML that might not be in Document API."""
        try:
            # Find all datasources in XML
            for datasource in xml_root.findall('.//datasource'):
                datasource_name = datasource.get('name', 'Unknown')
                
                # Extract connection information
                connection_info = self._extract_connection_info_from_xml(datasource)
                if connection_info:
                    self.field_definitions["data_source_relationships"][datasource_name]["connection_info"] = connection_info
                
                # Extract detailed field information from XML
                for column in datasource.findall('.//column'):
                    self._extract_column_details_from_xml(column, datasource_name)
                    
        except Exception as e:
            print(f"Error extracting from XML: {e}")
    
    def _extract_connection_info_from_xml(self, datasource_elem) -> Dict[str, Any]:
        """Extract connection information from datasource XML element."""
        connection_info = {}
        
        # Check for connection element
        connection = datasource_elem.find('.//connection')
        if connection is not None:
            connection_info = {
                "server": connection.get('server', ''),
                "dbname": connection.get('dbname', ''),
                "username": connection.get('username', ''),
                "authentication": connection.get('authentication', ''),
                "class": connection.get('class', ''),
                "port": connection.get('port', ''),
                "schema": connection.get('schema', ''),
                "table": connection.get('table', ''),
                "query": connection.get('query', ''),
                "custom_sql": connection.get('custom-sql', '')
            }
        
        # Check for inline data
        if datasource_elem.get('inline') == 'true':
            connection_info["type"] = "inline"
            connection_info["has_connection"] = datasource_elem.get('hasconnection', 'false') == 'true'
        
        return connection_info
    
    def _extract_column_details_from_xml(self, column_elem, datasource_name: str):
        """Extract detailed column information from XML column element."""
        column_name = column_elem.get('name', '')
        if not column_name:
            return
        
        # Find the corresponding field definition or create a new one
        field_def = self._find_field_definition(column_name, datasource_name)
        if not field_def:
            # Create a new field definition from XML
            field_def = {
                "name": column_name,
                "caption": column_elem.get('caption', ''),
                "datatype": column_elem.get('datatype', 'unknown'),
                "role": column_elem.get('role', 'unknown'),
                "type": column_elem.get('type', 'unknown'),
                "datasource": datasource_name,
                "api_attributes": {},
                "xml_attributes": {},
                "usage_info": {
                    "used_in_worksheets": [],
                    "used_in_dashboards": [],
                    "filter_usage": [],
                    "calculation_dependencies": []
                }
            }
            
            # Add to appropriate category
            if self._is_parameter_from_xml(column_elem):
                self.field_definitions["field_categories"]["parameters"].append(field_def)
            elif self._is_calculated_field_from_xml(column_elem):
                self.field_definitions["field_categories"]["calculated_fields"].append(field_def)
            elif self._is_table_field_from_xml(column_elem):
                self.field_definitions["field_categories"]["table_fields"].append(field_def)
            elif self._is_internal_field_from_xml(column_elem):
                self.field_definitions["field_categories"]["internal_fields"].append(field_def)
            else:
                self.field_definitions["field_categories"]["regular_fields"].append(field_def)
        
        # Extract XML-specific attributes
        xml_attrs = {}
        for attr_name, attr_value in column_elem.attrib.items():
            xml_attrs[attr_name] = attr_value
        
        field_def["xml_attributes"] = xml_attrs
        
        # Extract calculation details if present
        calculation = column_elem.find('.//calculation')
        if calculation is not None:
            field_def["calculation_details"] = {
                "class": calculation.get('class', ''),
                "formula": calculation.get('formula', ''),
                "xml_formula": calculation.text if calculation.text else ''
            }
        
        # Extract parameter details if present
        if 'param-domain-type' in column_elem.attrib:
            field_def["parameter_details"] = {
                "domain_type": column_elem.get('param-domain-type', ''),
                "current_value": column_elem.get('value', ''),
                "allowed_values": self._extract_parameter_values(column_elem)
            }
    
    def _extract_parameter_values(self, column_elem) -> Dict[str, Any]:
        """Extract parameter value constraints from XML."""
        values_info = {
            "type": "unknown",
            "values": [],
            "range": None,
            "list": []
        }
        
        # Look for value elements
        for value_elem in column_elem.findall('.//value'):
            value_info = {
                "value": value_elem.get('value', ''),
                "caption": value_elem.get('caption', ''),
                "null": value_elem.get('null', 'false') == 'true'
            }
            values_info["values"].append(value_info)
        
        # Look for range information
        range_elem = column_elem.find('.//range')
        if range_elem is not None:
            values_info["range"] = {
                "min": range_elem.get('min', ''),
                "max": range_elem.get('max', ''),
                "step": range_elem.get('step', '')
            }
        
        return values_info
    
    def _find_field_definition(self, field_name: str, datasource_name: str) -> Optional[Dict[str, Any]]:
        """Find a field definition by name and datasource."""
        for category in self.field_definitions["field_categories"].values():
            for field_def in category:
                if (field_def.get("name") == field_name and 
                    field_def.get("datasource") == datasource_name):
                    return field_def
        return None
    
    def _is_parameter_from_api(self, field) -> bool:
        """Check if field is a parameter using Document API."""
        try:
            # Check XML attributes for parameter-specific markers
            if hasattr(field, 'xml') and field.xml is not None:
                return 'param-domain-type' in field.xml.attrib
            return False
        except:
            return False
    
    def _is_calculated_field_from_api(self, field) -> bool:
        """Check if field is a calculated field using Document API."""
        try:
            return field.calculation is not None
        except:
            return False
    
    def _is_table_field_from_api(self, field) -> bool:
        """Check if field represents a table using Document API."""
        try:
            return field.datatype == 'table'
        except:
            return False
    
    def _is_internal_field_from_api(self, field) -> bool:
        """Check if field is an internal Tableau field using Document API."""
        try:
            name = field.id if hasattr(field, 'id') else field.name
            return (name.startswith('__tableau_') or 
                   name.startswith('_.fcp.') or
                   'internal' in name.lower())
        except:
            return False
    
    def _is_parameter_from_xml(self, column_elem) -> bool:
        """Check if field is a parameter based on XML attributes."""
        return 'param-domain-type' in column_elem.attrib
    
    def _is_calculated_field_from_xml(self, column_elem) -> bool:
        """Check if field is a calculated field based on XML attributes."""
        return column_elem.find('.//calculation') is not None
    
    def _is_table_field_from_xml(self, column_elem) -> bool:
        """Check if field represents a table based on XML attributes."""
        return column_elem.get('datatype', '') == 'table'
    
    def _is_internal_field_from_xml(self, column_elem) -> bool:
        """Check if field is an internal Tableau field based on XML attributes."""
        name = column_elem.get('name', '')
        return (name.startswith('__tableau_') or 
               name.startswith('_.fcp.') or
               'internal' in name.lower())
    
    def _analyze_field_relationships(self):
        """Analyze relationships between fields and their usage."""
        try:
            # Count field types
            field_counts = {}
            for category, fields in self.field_definitions["field_categories"].items():
                field_counts[category] = len(fields)
            
            self.field_definitions["field_statistics"] = {
                "total_fields": sum(field_counts.values()),
                "field_counts": field_counts,
                "datasource_count": len(self.field_definitions["data_source_relationships"]),
                "extraction_completeness": self._assess_extraction_completeness()
            }
            
        except Exception as e:
            print(f"Error analyzing field relationships: {e}")
    
    def _assess_extraction_completeness(self) -> Dict[str, Any]:
        """Assess how complete the field extraction is."""
        completeness = {
            "has_api_data": False,
            "has_xml_data": False,
            "has_calculations": False,
            "has_parameters": False,
            "has_connections": False,
            "missing_data": []
        }
        
        # Check for API data
        if any(self.field_definitions["field_categories"].values()):
            completeness["has_api_data"] = True
        
        # Check for XML data
        if any(field.get("xml_attributes") for category in self.field_definitions["field_categories"].values() 
               for field in category):
            completeness["has_xml_data"] = True
        
        # Check for calculations
        if self.field_definitions["field_categories"]["calculated_fields"]:
            completeness["has_calculations"] = True
        
        # Check for parameters
        if self.field_definitions["field_categories"]["parameters"]:
            completeness["has_parameters"] = True
        
        # Check for connections
        if any(ds.get("connection_info") for ds in self.field_definitions["data_source_relationships"].values()):
            completeness["has_connections"] = True
        
        return completeness
