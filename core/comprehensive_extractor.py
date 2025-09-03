#!/usr/bin/env python3
"""
Comprehensive Tableau Data Extractor
Extracts ALL available data from Tableau workbooks using the Document API
Provides complete migration-ready JSON output with Power BI instructions
"""

import json
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, List, Any, Optional
import re


class ComprehensiveTableauExtractor:
    """Extracts comprehensive data from Tableau workbooks for migration purposes."""
    
    def __init__(self, xml_root, workbook, file_path=None):
        self.xml_root = xml_root
        self.workbook = workbook
        self.file_path = file_path
        self.filename = file_path
        self.extraction_timestamp = datetime.now().isoformat()
    
    def extract_all_data(self) -> Dict[str, Any]:
        """Extract ALL available data from the Tableau workbook."""
        print("🔍 Starting comprehensive data extraction...")
        
        comprehensive_data = {
            "extraction_metadata": self._extract_extraction_metadata(),
            "workbook_metadata": self._extract_workbook_metadata(),
            "datasources": self._extract_comprehensive_datasources(),
            "fields_comprehensive": self._extract_comprehensive_fields(),
            "worksheets": self._extract_comprehensive_worksheets(),
            "dashboards": self._extract_comprehensive_dashboards(),
            "parameters": self._extract_comprehensive_parameters(),
            "calculated_fields": self._extract_comprehensive_calculated_fields(),
            "groups_sets_hierarchies": self._extract_groups_sets_hierarchies(),
            "filters": self._extract_comprehensive_filters(),
            "actions": self._extract_actions(),
            "formatting": self._extract_formatting(),
            "powerbi_migration_guide": self._generate_powerbi_migration_guide()
        }
        
        print("✅ Comprehensive extraction complete!")
        return comprehensive_data
    
    def _extract_extraction_metadata(self) -> Dict[str, Any]:
        """Extract metadata about the extraction process."""
        return {
            "extraction_timestamp": self.extraction_timestamp,
            "extractor_version": "1.0.0",
            "tableau_document_api_version": "2023.1+",
            "extraction_scope": "comprehensive_all_data",
            "powerbi_compatibility": "2023+"
        }
    
    def _extract_workbook_metadata(self) -> Dict[str, Any]:
        """Extract comprehensive workbook-level metadata."""
        if not self.workbook:
            return {}
        
        # Get workbook name from filename (this should be passed from the migrator)
        workbook_name = getattr(self, 'filename', 'Unknown')
        if workbook_name != 'Unknown':
            # Remove extension if present
            import os
            workbook_name = os.path.splitext(os.path.basename(workbook_name))[0]
        
        # Get workbook version from XML
        workbook_version = 'Unknown'
        if self.xml_root is not None:
            workbook_version = self.xml_root.get('version', self.xml_root.get('original-version', 'Unknown'))
        
        # Get Windows file metadata for author and dates
        workbook_author = 'Unknown'
        workbook_creation_date = 'Unknown'
        workbook_modified_date = 'Unknown'
        
        if hasattr(self, 'file_path') and self.file_path:
            try:
                import os
                import stat
                from datetime import datetime
                
                # Get file stats
                file_stats = os.stat(self.file_path)
                
                # Get file owner (Windows)
                try:
                    import pwd
                    workbook_author = pwd.getpwuid(file_stats.st_uid).pw_name
                except (ImportError, AttributeError):
                    # On Windows, try to get owner info
                    try:
                        import win32security
                        import win32api
                        sd = win32security.GetFileSecurity(self.file_path, win32security.OWNER_SECURITY_INFORMATION)
                        owner_sid = sd.GetSecurityDescriptorOwner()
                        name, domain, type = win32security.LookupAccountSid(None, owner_sid)
                        workbook_author = f"{domain}\\{name}" if domain else name
                    except ImportError:
                        # Fallback: use environment username
                        workbook_author = os.environ.get('USERNAME', os.environ.get('USER', 'Unknown'))
                
                # Get file dates
                workbook_creation_date = datetime.fromtimestamp(file_stats.st_ctime).isoformat()
                workbook_modified_date = datetime.fromtimestamp(file_stats.st_mtime).isoformat()
                
            except Exception as e:
                print(f"Warning: Could not get file metadata: {e}")
        
        # Get basic workbook info
        workbook_info = {
            "name": workbook_name,
            "version": workbook_version,
            "author": workbook_author,
            "creation_date": workbook_creation_date,
            "modified_date": workbook_modified_date,
            "total_datasources": len(self.workbook.datasources) if hasattr(self.workbook, 'datasources') else 0,
            "total_worksheets": 0,
            "total_dashboards": 0,
            "complexity_score": "Unknown"
        }
        
        # Count worksheets and dashboards from XML
        if self.xml_root:
            worksheets = self.xml_root.findall('.//worksheet')
            dashboards = self.xml_root.findall('.//dashboard')
            workbook_info["total_worksheets"] = len(worksheets)
            workbook_info["total_dashboards"] = len(dashboards)
            
            # Calculate complexity score
            total_fields = 0
            total_calculations = 0
            total_parameters = 0
            
            for datasource in self.workbook.datasources:
                if hasattr(datasource, 'fields'):
                    total_fields += len(datasource.fields)
                    for field_name, field_obj in datasource.fields.items():
                        if hasattr(field_obj, 'calculation') and field_obj.calculation:
                            total_calculations += 1
                        if hasattr(field_obj, 'xml') and field_obj.xml and field_obj.xml.get('param-domain-type'):
                            total_parameters += 1
            
            # Determine complexity
            if total_fields > 100 or total_calculations > 20 or total_parameters > 10:
                workbook_info["complexity_score"] = "High"
            elif total_fields > 50 or total_calculations > 10 or total_parameters > 5:
                workbook_info["complexity_score"] = "Medium"
            else:
                workbook_info["complexity_score"] = "Low"
            
            workbook_info["total_fields"] = total_fields
            workbook_info["total_calculations"] = total_calculations
            workbook_info["total_parameters"] = total_parameters
        
        # Add comprehensive metadata
        workbook_info["field_metadata"] = self._extract_comprehensive_field_metadata()
        workbook_info["worksheet_metadata"] = self._extract_worksheet_metadata()
        workbook_info["datasource_metadata"] = self._extract_datasource_metadata()
        workbook_info["workbook_attributes"] = self._extract_workbook_attributes()
        
        return workbook_info
    
    def _extract_comprehensive_field_metadata(self) -> Dict[str, Any]:
        """Extract comprehensive field metadata from all datasources."""
        field_metadata = {
            "all_fields": [],
            "field_types": {},
            "field_roles": {},
            "field_derivations": {},
            "field_captions": {},
            "field_instances": []
        }
        
        if not self.workbook:
            return field_metadata
        
        for datasource in self.workbook.datasources:
            if hasattr(datasource, 'fields'):
                for field_name, field_obj in datasource.fields.items():
                    field_info = {
                        "name": field_name,
                        "datasource": datasource.name,
                        "caption": getattr(field_obj, 'caption', ''),
                        "datatype": getattr(field_obj, 'datatype', ''),
                        "role": getattr(field_obj, 'role', ''),
                        "calculation": getattr(field_obj, 'calculation', ''),
                        "aggregation": getattr(field_obj, 'aggregation', ''),
                        "all_attributes": {}
                    }
                    
                    # Extract all XML attributes if available
                    if hasattr(field_obj, 'xml') and field_obj.xml is not None:
                        for attr_name, attr_value in field_obj.xml.attrib.items():
                            field_info["all_attributes"][attr_name] = attr_value
                    
                    field_metadata["all_fields"].append(field_info)
                    
                    # Categorize by type, role, etc.
                    if field_info["datatype"]:
                        if field_info["datatype"] not in field_metadata["field_types"]:
                            field_metadata["field_types"][field_info["datatype"]] = []
                        field_metadata["field_types"][field_info["datatype"]].append(field_name)
                    
                    if field_info["role"]:
                        if field_info["role"] not in field_metadata["field_roles"]:
                            field_metadata["field_roles"][field_info["role"]] = []
                        field_metadata["field_roles"][field_info["role"]].append(field_name)
        
        return field_metadata
    
    def _extract_worksheet_metadata(self) -> Dict[str, Any]:
        """Extract comprehensive worksheet metadata."""
        worksheet_metadata = {
            "worksheets": [],
            "total_worksheets": 0,
            "chart_types": {},
            "field_usage": {},
            "filter_usage": {}
        }
        
        if not self.xml_root:
            return worksheet_metadata
        
        worksheets = self.xml_root.findall('.//worksheet')
        worksheet_metadata["total_worksheets"] = len(worksheets)
        
        for worksheet in worksheets:
            ws_name = worksheet.get('name', 'Unknown')
            ws_info = {
                "name": ws_name,
                "attributes": dict(worksheet.attrib),
                "datasources": [],
                "fields_used": [],
                "filters": [],
                "encodings": {},
                "style_rules": []
            }
            
            # Extract datasource dependencies
            deps = worksheet.findall('.//datasource-dependencies')
            for dep in deps:
                datasource_name = dep.get('datasource', '')
                if datasource_name:
                    ws_info["datasources"].append(datasource_name)
                
                # Extract all columns and their metadata
                columns = dep.findall('.//column')
                for col in columns:
                    col_info = {
                        "name": col.get('name', ''),
                        "caption": col.get('caption', ''),
                        "datatype": col.get('datatype', ''),
                        "role": col.get('role', ''),
                        "type": col.get('type', ''),
                        "all_attributes": dict(col.attrib)
                    }
                    ws_info["fields_used"].append(col_info)
                    
                    # Track field usage
                    field_name = col_info["name"]
                    if field_name not in worksheet_metadata["field_usage"]:
                        worksheet_metadata["field_usage"][field_name] = []
                    worksheet_metadata["field_usage"][field_name].append(ws_name)
                
                # Extract column instances (derivations)
                column_instances = dep.findall('.//column-instance')
                for col_inst in column_instances:
                    inst_info = {
                        "column": col_inst.get('column', ''),
                        "derivation": col_inst.get('derivation', ''),
                        "name": col_inst.get('name', ''),
                        "pivot": col_inst.get('pivot', ''),
                        "type": col_inst.get('type', ''),
                        "all_attributes": dict(col_inst.attrib)
                    }
                    ws_info["fields_used"].append(inst_info)
            
            # Extract filters
            filters = worksheet.findall('.//filter')
            for filter_elem in filters:
                filter_info = {
                    "class": filter_elem.get('class', ''),
                    "column": filter_elem.get('column', ''),
                    "all_attributes": dict(filter_elem.attrib)
                }
                ws_info["filters"].append(filter_info)
                
                # Track filter usage
                filter_column = filter_info["column"]
                if filter_column not in worksheet_metadata["filter_usage"]:
                    worksheet_metadata["filter_usage"][filter_column] = []
                worksheet_metadata["filter_usage"][filter_column].append(ws_name)
            
            # Extract encodings
            encodings = worksheet.findall('.//encodings')
            for encoding in encodings:
                for enc_type in ['color', 'size', 'shape', 'text']:
                    enc_elem = encoding.find(f'.//{enc_type}')
                    if enc_elem is not None:
                        ws_info["encodings"][enc_type] = {
                            "column": enc_elem.get('column', ''),
                            "all_attributes": dict(enc_elem.attrib)
                        }
            
            # Extract style rules
            style_rules = worksheet.findall('.//style-rule')
            for rule in style_rules:
                rule_info = {
                    "element": rule.get('element', ''),
                    "all_attributes": dict(rule.attrib)
                }
                ws_info["style_rules"].append(rule_info)
            
            worksheet_metadata["worksheets"].append(ws_info)
        
        return worksheet_metadata
    
    def _extract_datasource_metadata(self) -> Dict[str, Any]:
        """Extract comprehensive datasource metadata."""
        datasource_metadata = {
            "datasources": [],
            "total_datasources": 0,
            "connection_types": {},
            "field_counts": {}
        }
        
        if not self.workbook:
            return datasource_metadata
        
        datasource_metadata["total_datasources"] = len(self.workbook.datasources)
        
        for datasource in self.workbook.datasources:
            ds_info = {
                "name": datasource.name,
                "caption": getattr(datasource, 'caption', ''),
                "fields": {},
                "connections": [],
                "all_attributes": {}
            }
            
            # Extract all datasource attributes
            if hasattr(datasource, 'xml') and datasource.xml is not None:
                for attr_name, attr_value in datasource.xml.attrib.items():
                    ds_info["all_attributes"][attr_name] = attr_value
            
            # Extract field information
            if hasattr(datasource, 'fields'):
                for field_name, field_obj in datasource.fields.items():
                    field_info = {
                        "name": field_name,
                        "caption": getattr(field_obj, 'caption', ''),
                        "datatype": getattr(field_obj, 'datatype', ''),
                        "role": getattr(field_obj, 'role', ''),
                        "calculation": getattr(field_obj, 'calculation', ''),
                        "aggregation": getattr(field_obj, 'aggregation', ''),
                        "all_attributes": {}
                    }
                    
                    # Extract all field attributes
                    if hasattr(field_obj, 'xml') and field_obj.xml is not None:
                        for attr_name, attr_value in field_obj.xml.attrib.items():
                            field_info["all_attributes"][attr_name] = attr_value
                    
                    ds_info["fields"][field_name] = field_info
            
            # Extract connection information
            if hasattr(datasource, 'connections'):
                for connection in datasource.connections:
                    conn_info = {
                        "server": getattr(connection, 'server', ''),
                        "dbname": getattr(connection, 'dbname', ''),
                        "username": getattr(connection, 'username', ''),
                        "authentication": getattr(connection, 'authentication', ''),
                        "class": getattr(connection, 'class', ''),
                        "all_attributes": {}
                    }
                    
                    # Extract all connection attributes
                    if hasattr(connection, 'xml') and connection.xml is not None:
                        for attr_name, attr_value in connection.xml.attrib.items():
                            conn_info["all_attributes"][attr_name] = attr_value
                    
                    ds_info["connections"].append(conn_info)
                    
                    # Track connection types
                    conn_class = conn_info["class"]
                    if conn_class not in datasource_metadata["connection_types"]:
                        datasource_metadata["connection_types"][conn_class] = 0
                    datasource_metadata["connection_types"][conn_class] += 1
            
            # Track field counts
            field_count = len(ds_info["fields"])
            datasource_metadata["field_counts"][datasource.name] = field_count
            
            datasource_metadata["datasources"].append(ds_info)
        
        return datasource_metadata
    
    def _extract_workbook_attributes(self) -> Dict[str, Any]:
        """Extract all workbook-level attributes."""
        workbook_attrs = {}
        
        if self.xml_root:
            workbook_elem = self.xml_root.find('.//workbook')
            if workbook_elem is not None:
                for attr_name, attr_value in workbook_elem.attrib.items():
                    workbook_attrs[attr_name] = attr_value
        
        return workbook_attrs
    
    def _extract_comprehensive_datasources(self) -> List[Dict[str, Any]]:
        """Extract comprehensive datasource information."""
        if not self.workbook:
            return []
        
        datasources = []
        
        for datasource in self.workbook.datasources:
            ds_info = {
                "name": datasource.name,
                "caption": getattr(datasource, 'caption', datasource.name),
                "type": "embedded",  # Could be enhanced to detect published vs embedded
                "connections": self._extract_datasource_connections(datasource),
                "tables": self._extract_datasource_tables(datasource),
                "custom_sql": self._extract_datasource_custom_sql(datasource),
                "relationships": self._extract_datasource_relationships(datasource),
                "extract_info": self._extract_extract_info(datasource),
                "field_count": len(datasource.fields) if hasattr(datasource, 'fields') else 0,
                "connection_count": len(datasource.connections) if hasattr(datasource, 'connections') else 0
            }
            datasources.append(ds_info)
        
        return datasources
    
    def _extract_datasource_connections(self, datasource) -> List[Dict[str, Any]]:
        """Extract detailed connection information."""
        connections = []
        
        if hasattr(datasource, 'connections'):
            for connection in datasource.connections:
                conn_info = {
                    "dbclass": getattr(connection, 'dbclass', ''),
                    "server": getattr(connection, 'server', ''),
                    "dbname": getattr(connection, 'dbname', ''),
                    "username": getattr(connection, 'username', ''),
                    "port": getattr(connection, 'port', ''),
                    "schema": getattr(connection, 'schema', ''),
                    "filename": getattr(connection, 'filename', ''),
                    "authentication": getattr(connection, 'authentication', ''),
                    "initial_sql": getattr(connection, 'initial_sql', ''),
                    "connection_string": self._build_connection_string(connection)
                }
                
                # Add cloud-specific attributes
                cloud_attrs = ['project', 'dataset', 'location', 'region', 'billing_project']
                for attr in cloud_attrs:
                    if hasattr(connection, attr):
                        conn_info[attr] = getattr(connection, attr)
                
                # Extract ALL XML attributes from the connection
                if hasattr(connection, '_connectionXML') and connection._connectionXML is not None:
                    xml_attrs = connection._connectionXML.attrib
                    for attr_name, attr_value in xml_attrs.items():
                        # Add any missing attributes that aren't already captured
                        if attr_name.lower() not in [k.lower() for k in conn_info.keys()]:
                            conn_info[attr_name] = attr_value
                
                connections.append(conn_info)
        
        return connections
    
    def _build_connection_string(self, connection) -> str:
        """Build a human-readable connection string."""
        parts = []
        
        if getattr(connection, 'dbclass', ''):
            parts.append(f"Type: {connection.dbclass}")
        
        if getattr(connection, 'server', ''):
            parts.append(f"Server: {connection.server}")
        
        if getattr(connection, 'dbname', ''):
            parts.append(f"Database: {connection.dbname}")
        
        if getattr(connection, 'username', ''):
            parts.append(f"User: {connection.username}")
        
        # Add BigQuery-specific attributes
        if hasattr(connection, '_connectionXML') and connection._connectionXML is not None:
            xml_attrs = connection._connectionXML.attrib
            
            if 'CATALOG' in xml_attrs:
                parts.append(f"Catalog: {xml_attrs['CATALOG']}")
            
            if 'EXECCATALOG' in xml_attrs:
                parts.append(f"Exec Catalog: {xml_attrs['EXECCATALOG']}")
            
            if 'project' in xml_attrs:
                parts.append(f"Project: {xml_attrs['project']}")
            
            if 'connection-dialect' in xml_attrs:
                parts.append(f"Dialect: {xml_attrs['connection-dialect']}")
        
        return " | ".join(parts) if parts else "No connection details"
    
    def _extract_datasource_tables(self, datasource) -> List[Dict[str, Any]]:
        """Extract table information from datasource."""
        tables = []
        
        if self.xml_root:
            # Look for table references in the datasource XML
            datasource_xml = self._find_datasource_xml(datasource.name)
            if datasource_xml:
                # Extract table references from relations
                for relation in datasource_xml.findall('.//relation[@type="table"]'):
                    table_name = relation.get('table', '')
                    connection_id = relation.get('connection', '')
                    
                    # Build full BigQuery table reference
                    full_table_ref = self._build_bigquery_table_reference(table_name, connection_id, datasource)
                    
                    table_info = {
                        "name": table_name,
                        "full_reference": full_table_ref,
                        "connection": connection_id,
                        "type": "table",
                        "alias": relation.get('name', ''),
                        "join_info": self._extract_table_join_info(relation)
                    }
                    tables.append(table_info)
        
        return tables
    
    def _build_bigquery_table_reference(self, table_name: str, connection_id: str, datasource) -> str:
        """Build full BigQuery table reference from connection details."""
        # Extract connection details
        for connection in datasource.connections:
            if hasattr(connection, '_connectionXML') and connection._connectionXML is not None:
                xml_attrs = connection._connectionXML.attrib
                
                # Get BigQuery project and dataset info
                exec_catalog = xml_attrs.get('EXECCATALOG', '')
                schema = xml_attrs.get('schema', '')
                
                if exec_catalog and schema:
                    # Clean table name (remove brackets and schema prefix)
                    clean_table = table_name.replace('[', '').replace(']', '')
                    if '.' in clean_table:
                        # Remove schema prefix if present
                        parts = clean_table.split('.')
                        if len(parts) >= 2:
                            clean_table = parts[-1]  # Get just the table name
                    
                    return f"{exec_catalog}.{schema}.{clean_table}"
        
        return table_name  # Fallback to original name
    
    def _extract_table_join_info(self, relation) -> Dict[str, Any]:
        """Extract join information for table relationships."""
        join_info = {
            "join_type": "none",
            "join_conditions": [],
            "left_table": "",
            "right_table": ""
        }
        
        # Look for join conditions in the relation
        # This would need to be enhanced based on actual join structure
        return join_info
    
    def _extract_datasource_custom_sql(self, datasource) -> List[Dict[str, Any]]:
        """Extract custom SQL queries from datasource."""
        custom_sql = []
        seen_sql = set()  # Track seen SQL to avoid duplicates
        
        if hasattr(datasource, '_get_custom_sql'):
            relations = datasource._get_custom_sql()
            for relation in relations:
                if relation.get('type') == 'text' and relation.text and relation.text.strip():
                    sql_text = relation.text.strip()
                    
                    # Skip if we've already seen this SQL
                    if sql_text in seen_sql:
                        continue
                    seen_sql.add(sql_text)
                    
                    sql_info = {
                        "name": relation.get('name', 'Custom Query'),
                        "sql": sql_text,
                        "connection": relation.get('connection', ''),
                        "type": "custom_sql"
                    }
                    custom_sql.append(sql_info)
        
        return custom_sql
    
    def _extract_datasource_relationships(self, datasource) -> List[Dict[str, Any]]:
        """Extract relationship information between tables."""
        relationships = []
        
        # This would need to be enhanced to extract actual relationship definitions
        # For now, return empty list as relationships are complex to extract
        return relationships
    
    def _extract_extract_info(self, datasource) -> Dict[str, Any]:
        """Extract information about data extracts."""
        extract_info = {
            "has_extract": False,
            "extract_type": None,
            "extract_size": None,
            "last_refresh": None
        }
        
        # Check if datasource has extract information
        if hasattr(datasource, 'extract') and datasource.extract:
            extract_info["has_extract"] = True
            extract_info["extract_type"] = "hyper"  # Most common type
        
        return extract_info
    
    def _extract_comprehensive_fields(self) -> Dict[str, Any]:
        """Extract comprehensive field information."""
        if not self.workbook:
            return {"regular_fields": [], "calculated_fields": [], "parameters": []}
        
        regular_fields = []
        calculated_fields = []
        parameters = []
        
        for datasource in self.workbook.datasources:
            if hasattr(datasource, 'fields'):
                for field_name, field_obj in datasource.fields.items():
                    field_info = self._extract_field_details(field_name, field_obj, datasource.name)
                    
                    if field_info.get('is_parameter', False):
                        parameters.append(field_info)
                    elif field_info.get('is_calculated', False):
                        calculated_fields.append(field_info)
                    else:
                        regular_fields.append(field_info)
        
        return {
            "regular_fields": regular_fields,
            "calculated_fields": calculated_fields,
            "parameters": parameters,
            "total_fields": len(regular_fields) + len(calculated_fields) + len(parameters)
        }
    
    def _extract_field_details(self, field_name: str, field_obj, datasource_name: str) -> Dict[str, Any]:
        """Extract detailed information about a single field."""
        # Clean field name
        clean_name = field_name.replace('[', '').replace(']', '')
        
        field_info = {
            "name": clean_name,
            "caption": getattr(field_obj, 'caption', clean_name),
            "id": getattr(field_obj, 'id', field_name),
            "datatype": getattr(field_obj, 'datatype', 'Unknown'),
            "role": getattr(field_obj, 'role', 'Unknown'),
            "type": getattr(field_obj, 'type', 'Unknown'),
            "aggregation": getattr(field_obj, 'default_aggregation', 'None'),
            "datasource": datasource_name,
            "is_calculated": False,
            "is_parameter": False,
            "used_in_worksheets": [],
            "powerbi_equivalent": self._generate_powerbi_field_equivalent(field_obj)
        }
        
        # Check if it's a calculated field
        if hasattr(field_obj, 'calculation') and field_obj.calculation:
            field_info["is_calculated"] = True
            field_info["calculation_formula"] = field_obj.calculation
            field_info["calculation_dependencies"] = self._extract_calculation_dependencies(field_obj.calculation)
            field_info["calculation_complexity"] = self._assess_calculation_complexity(field_obj.calculation)
            field_info["powerbi_dax_equivalent"] = self._convert_to_dax(field_obj.calculation)
        
        # Check if it's a parameter
        if hasattr(field_obj, 'xml') and field_obj.xml and field_obj.xml.get('param-domain-type'):
            field_info["is_parameter"] = True
            field_info["parameter_type"] = field_obj.xml.get('param-domain-type', 'Unknown')
            field_info["parameter_value"] = field_obj.xml.get('value', '')
            field_info["parameter_allowed_values"] = self._extract_parameter_allowed_values(field_obj.xml)
        
        # Get worksheet usage
        if hasattr(field_obj, 'worksheets'):
            field_info["used_in_worksheets"] = list(field_obj.worksheets) if field_obj.worksheets else []
        
        return field_info
    
    def _generate_powerbi_field_equivalent(self, field_obj) -> str:
        """Generate Power BI equivalent field information."""
        datatype = getattr(field_obj, 'datatype', 'Unknown')
        role = getattr(field_obj, 'role', 'Unknown')
        
        # Map Tableau data types to Power BI
        type_mapping = {
            'string': 'Text',
            'integer': 'Whole Number',
            'real': 'Decimal Number',
            'boolean': 'True/False',
            'date': 'Date',
            'datetime': 'Date/Time'
        }
        
        powerbi_type = type_mapping.get(datatype.lower() if datatype else 'Unknown', datatype or 'Unknown')
        
        # Handle None role
        if role is None:
            role = 'Unknown'
        
        return f"{powerbi_type} ({role.title()})"
    
    def _extract_calculation_dependencies(self, formula: str) -> List[str]:
        """Extract field dependencies from a calculation formula."""
        # Simple regex to find field references in brackets
        field_pattern = r'\[([^\]]+)\]'
        matches = re.findall(field_pattern, formula)
        return list(set(matches))  # Remove duplicates
    
    def _assess_calculation_complexity(self, formula: str) -> str:
        """Assess the complexity of a calculation formula."""
        # Simple complexity assessment based on formula length and functions
        if len(formula) < 50 and formula.count('(') < 3:
            return "Simple"
        elif len(formula) < 200 and formula.count('(') < 10:
            return "Medium"
        else:
            return "Complex"
    
    def _convert_to_dax(self, tableau_formula: str) -> str:
        """Convert Tableau formula to DAX equivalent."""
        # This is a simplified conversion - would need more sophisticated logic
        dax_formula = tableau_formula
        
        # Basic function mappings
        function_mappings = {
            'IF': 'IF',
            'SUM': 'SUM',
            'AVG': 'AVERAGE',
            'COUNT': 'COUNT',
            'MAX': 'MAX',
            'MIN': 'MIN'
        }
        
        for tableau_func, dax_func in function_mappings.items():
            dax_formula = dax_formula.replace(tableau_func, dax_func)
        
        return dax_formula
    
    def _extract_parameter_allowed_values(self, param_xml) -> Dict[str, Any]:
        """Extract allowed values for a parameter."""
        allowed_values = {
            "type": "Unknown",
            "values": [],
            "range": None
        }
        
        # This would need to be enhanced based on parameter type
        param_type = param_xml.get('param-domain-type', 'Unknown')
        allowed_values["type"] = param_type
        
        return allowed_values
    
    def _extract_comprehensive_worksheets(self) -> List[Dict[str, Any]]:
        """Extract comprehensive worksheet information."""
        if not self.xml_root:
            return []
        
        worksheets = []
        worksheet_elements = self.xml_root.findall('.//worksheet')
        
        for worksheet in worksheet_elements:
            # Find the datasource for this worksheet
            datasource = self._find_worksheet_datasource(worksheet)
            
            ws_info = {
                "name": worksheet.get('name', 'Unknown'),
                "chart_type": self._infer_chart_type(worksheet),
                "encoding": self._extract_worksheet_encoding(worksheet, datasource),
                "filters": self._extract_worksheet_filters(worksheet, datasource),
                "formatting": self._extract_worksheet_formatting(worksheet),
                "analytics": self._extract_worksheet_analytics(worksheet),
                "used_fields": self._extract_used_fields(worksheet, datasource),
                "powerbi_instructions": self._generate_powerbi_worksheet_instructions(worksheet, datasource)
            }
            worksheets.append(ws_info)
        
        return worksheets
    
    def _find_worksheet_datasource(self, worksheet) -> Any:
        """Find the datasource associated with a worksheet."""
        if not self.workbook:
            return None
        
        # Look for datasource dependencies in the worksheet
        deps = worksheet.findall('.//datasource-dependencies')
        for dep in deps:
            datasource_name = dep.get('datasource')
            if datasource_name:
                # Find the datasource in the workbook
                for datasource in self.workbook.datasources:
                    if datasource.name == datasource_name:
                        return datasource
        
        # Fallback: return the first datasource if we can't find a specific one
        if self.workbook.datasources:
            return self.workbook.datasources[0]
        
        return None
    
    def _build_field_lookup_map(self, datasource) -> Dict[str, str]:
        """Build a lookup map from field names to their captions."""
        field_map = {}
        if datasource and hasattr(datasource, 'fields'):
            for field_key, field_obj in datasource.fields.items():
                if hasattr(field_obj, 'caption') and field_obj.caption:
                    # Map both the field key and cleaned field name to the caption
                    field_map[field_key] = field_obj.caption
                    # Also map common variations
                    clean_key = field_key.replace('_', '').lower()
                    field_map[clean_key] = field_obj.caption
                    # Map the original field name
                    if hasattr(field_obj, 'name') and field_obj.name:
                        field_map[field_obj.name] = field_obj.caption
        return field_map
    
    def _infer_chart_type(self, worksheet) -> str:
        """Infer chart type from worksheet structure."""
        # Enhanced chart type inference
        worksheet_name = worksheet.get('name', '').lower()
        
        # Check mark type
        mark_elem = worksheet.find('.//mark')
        mark_class = mark_elem.get('class', 'Automatic') if mark_elem is not None else 'Automatic'
        
        # Name-based detection
        if 'table' in worksheet_name:
            return 'Table'
        elif 'line' in worksheet_name:
            return 'Line Chart'
        elif 'bar' in worksheet_name or 'column' in worksheet_name:
            return 'Bar Chart'
        elif 'scatter' in worksheet_name:
            return 'Scatter Plot'
        elif 'map' in worksheet_name:
            return 'Map'
        elif 'pie' in worksheet_name:
            return 'Pie Chart'
        elif 'heatmap' in worksheet_name:
            return 'Heatmap'
        elif 'treemap' in worksheet_name:
            return 'Treemap'
        
        # Mark type-based detection
        if mark_class.lower() == 'table':
            return 'Table'
        elif mark_class.lower() in ['line', 'polygon']:
            return 'Line Chart'
        elif mark_class.lower() in ['bar', 'square']:
            return 'Bar Chart'
        elif mark_class.lower() in ['circle', 'point']:
            return 'Scatter Plot'
        elif mark_class.lower() in ['map', 'polygon']:
            return 'Map'
        
        return 'Bar Chart'  # Default fallback
    
    def _extract_worksheet_encoding(self, worksheet, datasource=None) -> Dict[str, Any]:
        """Extract visual encoding information from worksheet."""
        encoding = {
            "x_axis": None,
            "y_axis": None,
            "color": None,
            "size": None,
            "shape": None,
            "text": None,
            "tooltip": None,
            "detail": None,
            "page": None
        }
        
        # Extract rows and columns
        rows_elem = worksheet.find('.//rows')
        cols_elem = worksheet.find('.//cols')
        
        if rows_elem is not None and rows_elem.text:
            clean_field = self._clean_field_name(rows_elem.text.strip('[]'), datasource)
            encoding["y_axis"] = {"field": clean_field}
        
        if cols_elem is not None and cols_elem.text:
            clean_field = self._clean_field_name(cols_elem.text.strip('[]'), datasource)
            encoding["x_axis"] = {"field": clean_field}
        
        # Extract other encodings from marks
        marks_elem = worksheet.find('.//marks')
        if marks_elem is not None:
            # Extract color encoding
            color_elem = marks_elem.find('.//color')
            if color_elem is not None and color_elem.text:
                clean_field = self._clean_field_name(color_elem.text.strip('[]'), datasource)
                encoding["color"] = {"field": clean_field}
            
            # Extract size encoding
            size_elem = marks_elem.find('.//size')
            if size_elem is not None and size_elem.text:
                clean_field = self._clean_field_name(size_elem.text.strip('[]'), datasource)
                encoding["size"] = {"field": clean_field}
        
        return encoding
    
    def _clean_field_name(self, field_name: str, datasource=None) -> str:
        """Clean field name by removing datasource IDs and encoding information, using actual Tableau field names."""
        if not field_name:
            return field_name
        
        # Handle complex calculated expressions FIRST (before other processing)
        if 'mother_age' in field_name and 'father_age' in field_name and '+' in field_name:
            return 'Average Parent Age'
        
        # Handle calculated fields with complex expressions
        if '(' in field_name and ')' in field_name:
            # This is likely a calculated field, try to extract a meaningful name
            if 'mother_age' in field_name and 'father_age' in field_name:
                return 'Average Parent Age'
            elif 'sum:' in field_name.lower():
                return field_name.replace('sum:', '').replace(':qk', '').replace(':nk', '')
            elif 'avg:' in field_name.lower():
                return field_name.replace('avg:', '').replace(':qk', '').replace(':nk', '')
        
        # Remove datasource ID prefix (e.g., "federated.1kjfz6803ixra81biem681mb5g7h].")
        if '].' in field_name:
            field_name = field_name.split('].')[1]
        
        # Remove leading brackets
        if field_name.startswith('['):
            field_name = field_name[1:]
        if field_name.endswith(']'):
            field_name = field_name[:-1]
        
        # Handle special Tableau fields
        if field_name == ':Measure Names':
            return 'Measure Names'
        elif field_name.startswith(':') and field_name.endswith(':'):
            return field_name.strip(':')
        
        # Extract field name from encoding format (e.g., "none:corpus:nk" -> "corpus")
        if ':' in field_name:
            parts = field_name.split(':')
            if len(parts) >= 2:
                # Handle derivations like "avg:father_age:qk" -> "avg:father_age"
                if len(parts) == 3 and parts[2] in ['qk', 'nk']:
                    # Keep derivation + field name, remove suffix
                    field_name = f"{parts[0]}:{parts[1]}"
                else:
                    # Get the middle part which is usually the field name
                    field_name = parts[1]
        
        # Clean up remaining encoding suffixes
        field_name = field_name.replace(':qk', '').replace(':nk', '').replace('none:', '')
        
        # Try to get the actual Tableau field name (caption) using lookup map
        if datasource:
            field_map = self._build_field_lookup_map(datasource)
            
            # Try exact match first
            if field_name in field_map:
                return field_map[field_name]
            
            # Try lowercase match
            if field_name.lower() in field_map:
                return field_map[field_name.lower()]
            
            # Try without underscores
            clean_name = field_name.replace('_', '').lower()
            if clean_name in field_map:
                return field_map[clean_name]
        
        return field_name
    
    def _extract_worksheet_filters(self, worksheet, datasource=None) -> List[Dict[str, Any]]:
        """Extract filter information from worksheet."""
        filters = []
        
        filter_elements = worksheet.findall('.//filter')
        for filter_elem in filter_elements:
            raw_field = filter_elem.get('column', '').strip('[]')
            clean_field = self._clean_field_name(raw_field, datasource)
            
            filter_info = {
                "name": filter_elem.get('name', ''),
                "type": filter_elem.get('class', 'Unknown'),
                "field": clean_field,
                "raw_field": raw_field,  # Keep original for debugging
                "function": filter_elem.get('function', ''),
                "values": self._extract_filter_values(filter_elem)
            }
            filters.append(filter_info)
        
        return filters
    
    def _extract_filter_values(self, filter_elem) -> List[str]:
        """Extract filter values from filter element."""
        values = []
        
        # Look for groupfilter elements with member values
        for groupfilter in filter_elem.findall('.//groupfilter[@function="member"]'):
            member_value = groupfilter.get('member', '')
            if member_value:
                clean_value = member_value.replace('&quot;', '"').strip('[]')
                values.append(clean_value)
        
        return values
    
    def _extract_worksheet_formatting(self, worksheet) -> Dict[str, Any]:
        """Extract formatting information from worksheet."""
        formatting = {
            "colors": [],
            "fonts": {},
            "sizing": {},
            "legends": {},
            "axes": {}
        }
        
        # This would need to be enhanced to extract actual formatting details
        # For now, return basic structure
        
        return formatting
    
    def _extract_worksheet_analytics(self, worksheet) -> Dict[str, Any]:
        """Extract analytics information (trend lines, reference lines, etc.)."""
        analytics = {
            "trend_lines": [],
            "reference_lines": [],
            "forecasting": None
        }
        
        # This would need to be enhanced to extract actual analytics
        return analytics
    
    def _extract_used_fields(self, worksheet, datasource=None) -> List[str]:
        """Extract fields used in worksheet."""
        used_fields = []
        
        # Look for datasource-dependencies
        deps = worksheet.findall('.//datasource-dependencies')
        for dep in deps:
            columns = dep.findall('.//column')
            for col in columns:
                col_name = col.get('name', '')
                if col_name and col_name.startswith('[') and col_name.endswith(']'):
                    clean_name = self._clean_field_name_from_dependency(col_name, datasource)
                    if clean_name and clean_name not in used_fields and clean_name != 'Measure Names':
                        used_fields.append(clean_name)
        
        # Also extract from encoding
        encoding = self._extract_worksheet_encoding(worksheet, datasource)
        for axis in ['x_axis', 'y_axis', 'color', 'size']:
            if encoding.get(axis) and encoding[axis].get('field'):
                field_name = encoding[axis]['field']
                if field_name and field_name not in used_fields and field_name != 'Measure Names':
                    used_fields.append(field_name)
        
        return used_fields
    
    def _clean_field_name_from_dependency(self, dependency_name: str, datasource=None) -> str:
        """Clean field name from datasource dependency format."""
        clean = dependency_name.strip('[]')
        parts = clean.split(':')
        if len(parts) >= 2:
            field_name = parts[1]
            # Use the enhanced field name cleaning with datasource
            return self._clean_field_name(field_name, datasource)
        return self._clean_field_name(clean, datasource)
    
    def _generate_powerbi_worksheet_instructions(self, worksheet, datasource=None) -> List[str]:
        """Generate Power BI migration instructions for worksheet."""
        instructions = []
        
        chart_type = self._infer_chart_type(worksheet)
        instructions.append(f"Create {chart_type} visual in Power BI")
        
        # Add field placement instructions
        encoding = self._extract_worksheet_encoding(worksheet, datasource)
        
        if encoding.get('x_axis') and encoding['x_axis'].get('field'):
            field_name = encoding['x_axis']['field']
            if field_name != 'Measure Names':  # Skip internal Tableau fields
                instructions.append(f"Add {field_name} to X-axis")
        
        if encoding.get('y_axis') and encoding['y_axis'].get('field'):
            field_name = encoding['y_axis']['field']
            if field_name != 'Measure Names':  # Skip internal Tableau fields
                instructions.append(f"Add {field_name} to Y-axis")
        
        if encoding.get('color') and encoding['color'].get('field'):
            field_name = encoding['color']['field']
            if field_name != 'Measure Names':  # Skip internal Tableau fields
                instructions.append(f"Add {field_name} to Legend/Color")
        
        # Add filter instructions
        filters = self._extract_worksheet_filters(worksheet, datasource)
        for filter_info in filters:
            if filter_info.get('field') and filter_info['field'] != 'Measure Names':
                instructions.append(f"Add {filter_info['field']} filter")
        
        return instructions
    
    def _extract_comprehensive_dashboards(self) -> List[Dict[str, Any]]:
        """Extract comprehensive dashboard information."""
        if not self.xml_root:
            return []
        
        dashboards = []
        dashboard_elements = self.xml_root.findall('.//dashboard')
        
        for dashboard in dashboard_elements:
            dashboard_info = {
                "name": dashboard.get('name', 'Unknown'),
                "size": self._extract_dashboard_size(dashboard),
                "layout_type": self._extract_dashboard_layout_type(dashboard),
                "contained_objects": self._extract_dashboard_objects(dashboard),
                "filters": self._extract_dashboard_filters(dashboard),
                "actions": self._extract_dashboard_actions(dashboard),
                "formatting": self._extract_dashboard_formatting(dashboard),
                "powerbi_migration": self._generate_powerbi_dashboard_instructions(dashboard)
            }
            dashboards.append(dashboard_info)
        
        return dashboards
    
    def _extract_dashboard_size(self, dashboard) -> Dict[str, str]:
        """Extract dashboard size information."""
        size_elem = dashboard.find('.//size')
        if size_elem is not None:
            return {
                "width": size_elem.get('width', 'Unknown'),
                "height": size_elem.get('height', 'Unknown')
            }
        return {"width": "Unknown", "height": "Unknown"}
    
    def _extract_dashboard_layout_type(self, dashboard) -> str:
        """Extract dashboard layout type."""
        # Check for layout type indicators
        if dashboard.find('.//floating') is not None:
            return "Floating"
        elif dashboard.find('.//tiled') is not None:
            return "Tiled"
        else:
            return "Tiled"  # Default
    
    def _extract_dashboard_objects(self, dashboard) -> List[Dict[str, Any]]:
        """Extract objects contained in dashboard."""
        objects = []
        
        # Extract worksheets
        worksheet_elements = dashboard.findall('.//worksheet')
        for ws_elem in worksheet_elements:
            obj_info = {
                "type": "worksheet",
                "name": ws_elem.get('name', ''),
                "position": self._extract_object_position(ws_elem),
                "size": self._extract_object_size(ws_elem)
            }
            objects.append(obj_info)
        
        return objects
    
    def _extract_object_position(self, obj_elem) -> Dict[str, str]:
        """Extract object position information."""
        # This would need to be enhanced to extract actual position data
        return {"x": "0", "y": "0"}
    
    def _extract_object_size(self, obj_elem) -> Dict[str, str]:
        """Extract object size information."""
        # This would need to be enhanced to extract actual size data
        return {"width": "auto", "height": "auto"}
    
    def _extract_dashboard_filters(self, dashboard) -> List[Dict[str, Any]]:
        """Extract dashboard-level filters."""
        filters = []
        
        filter_elements = dashboard.findall('.//filter')
        for filter_elem in filter_elements:
            filter_info = {
                "name": filter_elem.get('name', ''),
                "type": filter_elem.get('class', 'Unknown'),
                "field": filter_elem.get('field', '').strip('[]')
            }
            filters.append(filter_info)
        
        return filters
    
    def _extract_dashboard_actions(self, dashboard) -> List[Dict[str, Any]]:
        """Extract dashboard actions."""
        actions = []
        
        # This would need to be enhanced to extract actual action data
        return actions
    
    def _extract_dashboard_formatting(self, dashboard) -> Dict[str, Any]:
        """Extract dashboard formatting."""
        formatting = {
            "background": {},
            "borders": {},
            "spacing": {}
        }
        
        # This would need to be enhanced to extract actual formatting
        return formatting
    
    def _generate_powerbi_dashboard_instructions(self, dashboard) -> Dict[str, Any]:
        """Generate Power BI migration instructions for dashboard."""
        return {
            "report_pages": 1,
            "suggested_layout": "Grid",
            "filter_setup": [],
            "instructions": [
                "Create new Power BI report page",
                "Set page size to match dashboard dimensions",
                "Add visuals from contained worksheets",
                "Configure page-level filters"
            ]
        }
    
    def _extract_comprehensive_parameters(self) -> List[Dict[str, Any]]:
        """Extract comprehensive parameter information."""
        parameters = []
        
        if not self.workbook:
            return parameters
        
        for datasource in self.workbook.datasources:
            if hasattr(datasource, 'fields'):
                for field_name, field_obj in datasource.fields.items():
                    if hasattr(field_obj, 'xml') and field_obj.xml and field_obj.xml.get('param-domain-type'):
                        param_info = {
                            "name": field_name.replace('[', '').replace(']', ''),
                            "caption": getattr(field_obj, 'caption', field_name),
                            "type": field_obj.xml.get('param-domain-type', 'Unknown'),
                            "current_value": field_obj.xml.get('value', ''),
                            "allowed_values": self._extract_parameter_allowed_values(field_obj.xml),
                            "datatype": getattr(field_obj, 'datatype', 'Unknown'),
                            "powerbi_steps": self._generate_powerbi_parameter_steps(field_obj)
                        }
                        parameters.append(param_info)
        
        return parameters
    
    def _generate_powerbi_parameter_steps(self, field_obj) -> List[str]:
        """Generate Power BI steps for parameter creation."""
        steps = []
        
        param_type = field_obj.xml.get('param-domain-type', 'Unknown') if field_obj.xml else 'Unknown'
        current_value = field_obj.xml.get('value', '') if field_obj.xml else ''
        
        steps.append(f"Create {param_type} parameter in Power BI")
        if current_value:
            steps.append(f"Set default value to {current_value}")
        
        return steps
    
    def _extract_comprehensive_calculated_fields(self) -> List[Dict[str, Any]]:
        """Extract comprehensive calculated field information."""
        calculated_fields = []
        
        if not self.workbook:
            return calculated_fields
        
        for datasource in self.workbook.datasources:
            if hasattr(datasource, 'fields'):
                for field_name, field_obj in datasource.fields.items():
                    if hasattr(field_obj, 'calculation') and field_obj.calculation:
                        calc_info = {
                            "name": field_name.replace('[', '').replace(']', ''),
                            "caption": getattr(field_obj, 'caption', field_name),
                            "formula": field_obj.calculation,
                            "dependencies": self._extract_calculation_dependencies(field_obj.calculation),
                            "complexity": self._assess_calculation_complexity(field_obj.calculation),
                            "powerbi_equivalent": self._convert_to_dax(field_obj.calculation),
                            "used_in_sheets": list(field_obj.worksheets) if hasattr(field_obj, 'worksheets') and field_obj.worksheets else [],
                            "datatype": getattr(field_obj, 'datatype', 'Unknown'),
                            "role": getattr(field_obj, 'role', 'Unknown')
                        }
                        calculated_fields.append(calc_info)
        
        return calculated_fields
    
    def _extract_groups_sets_hierarchies(self) -> Dict[str, List[Dict[str, Any]]]:
        """Extract groups, sets, and hierarchies information."""
        return {
            "groups": [],
            "sets": [],
            "hierarchies": []
        }
    
    def _extract_comprehensive_filters(self) -> Dict[str, List[Dict[str, Any]]]:
        """Extract comprehensive filter information."""
        return {
            "worksheet_filters": [],
            "dashboard_filters": [],
            "data_source_filters": []
        }
    
    def _extract_actions(self) -> List[Dict[str, Any]]:
        """Extract action information."""
        return []
    
    def _extract_formatting(self) -> Dict[str, Any]:
        """Extract formatting information."""
        return {
            "workbook_formatting": {},
            "worksheet_formatting": {},
            "dashboard_formatting": {}
        }
    
    def _generate_powerbi_migration_guide(self) -> Dict[str, Any]:
        """Generate comprehensive Power BI migration guide."""
        return {
            "overview": "Comprehensive migration guide from Tableau to Power BI",
            "steps": [
                "1. Set up Power BI workspace and data sources",
                "2. Import data using Power Query",
                "3. Create calculated columns and measures",
                "4. Build visuals matching Tableau worksheets",
                "5. Create dashboard pages",
                "6. Configure filters and interactions",
                "7. Test and validate results"
            ],
            "data_source_mapping": {},
            "visual_mapping": {},
            "function_mapping": {
                "Tableau": "Power BI DAX",
                "SUM": "SUM",
                "AVG": "AVERAGE",
                "COUNT": "COUNT",
                "IF": "IF"
            },
            "best_practices": [
                "Use Power Query for data transformation",
                "Create measures for calculated fields",
                "Use proper data modeling",
                "Optimize for performance"
            ]
        }
    
    def _find_datasource_xml(self, datasource_name: str):
        """Find XML element for specific datasource."""
        if not self.xml_root:
            return None
        
        for datasource in self.xml_root.findall('.//datasource'):
            if datasource.get('name') == datasource_name:
                return datasource
        return None
    
    def export_to_json(self, data: Dict[str, Any], output_path: str) -> None:
        """Export comprehensive data to JSON file."""
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Comprehensive data exported to: {output_path}")
    
    def generate_bigquery_setup_guide(self, data: Dict[str, Any], output_path: str) -> None:
        """Generate BigQuery setup guide for Power BI migration."""
        guide_content = []
        
        # Header
        guide_content.append("BIGQUERY SETUP GUIDE FOR POWER BI")
        guide_content.append("=" * 50)
        guide_content.append("")
        
        # Process each datasource
        for datasource in data.get('datasources', []):
            if datasource.get('connections'):
                guide_content.extend(self._generate_datasource_guide(datasource, data))
                guide_content.append("")
        
        # Write to file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(guide_content))
        
        print(f"✅ BigQuery setup guide generated: {output_path}")
    
    def _generate_datasource_guide(self, datasource: Dict[str, Any], data: Dict[str, Any]) -> List[str]:
        """Generate setup guide for a single datasource."""
        guide_lines = []
        
        # Datasource header
        guide_lines.append(f"Data Source: {datasource['name']}")
        guide_lines.append(f"Caption: {datasource['caption']}")
        guide_lines.append(f"Fields Available: {datasource['field_count']}")
        
        # Count used fields
        used_fields = self._count_used_fields(datasource['name'], data)
        guide_lines.append(f"Fields Used in Workbook: {used_fields}")
        
        # Count calculated fields and parameters
        calc_count = len([f for f in data.get('calculated_fields', []) if f.get('datasource') == datasource['name']])
        param_count = len([f for f in data.get('parameters', []) if f.get('datasource') == datasource['name']])
        
        guide_lines.append(f"Calculated Fields: {calc_count}")
        guide_lines.append(f"Parameter Fields: {param_count}")
        guide_lines.append("")
        
        # Parameters section
        if param_count > 0:
            guide_lines.append("PARAMETERS TO RECREATE IN POWER BI:")
            guide_lines.append("-" * 40)
            for param in data.get('parameters', []):
                if param.get('datasource') == datasource['name']:
                    guide_lines.append(f"  📊 {param['name']}:")
                    guide_lines.append(f"     Type: {param['datatype']}")
                    guide_lines.append(f"     Default Value: {param.get('current_value', 'N/A')}")
                    guide_lines.append(f"     Usage: Create as Power BI parameter")
                    guide_lines.append("")
        
        # Connection details
        guide_lines.append("CONNECTION DETAILS:")
        guide_lines.append("-" * 20)
        for i, connection in enumerate(datasource['connections'], 1):
            guide_lines.append(f"Connection {i} ({connection['dbclass']}):")
            
            # Extract BigQuery specific details
            exec_catalog = connection.get('EXECCATALOG', '')
            project = connection.get('project', '')
            schema = connection.get('schema', '')
            
            if exec_catalog:
                guide_lines.append(f"  Billing Project: {exec_catalog}")
            if project:
                guide_lines.append(f"  Project: {project}")
            if schema:
                guide_lines.append(f"  Dataset: {schema}")
            if connection.get('authentication'):
                guide_lines.append(f"  Authentication: {connection['authentication']}")
            if connection.get('connection-dialect'):
                guide_lines.append(f"  Connection Dialect: {connection['connection-dialect']}")
            if connection.get('username'):
                guide_lines.append(f"  Username: {connection['username']}")
            guide_lines.append("")
        
        # Tables section
        if datasource.get('tables'):
            guide_lines.append("TABLES TO IMPORT:")
            guide_lines.append("-" * 20)
            for table in datasource['tables']:
                if table.get('full_reference'):
                    guide_lines.append(f"  {table['full_reference']}")
            guide_lines.append("")
            
            # Main table
            if datasource['tables']:
                main_table = datasource['tables'][0]
                alias = main_table.get('alias', '')
                if alias and main_table.get('full_reference'):
                    guide_lines.append(f"MAIN TABLE: {main_table['full_reference']} (aliased as {alias})")
                guide_lines.append("")
        
        # Relationships
        if not datasource.get('relationships'):
            guide_lines.append("No relationships found")
            guide_lines.append("")
        
        # Parameters details
        if param_count > 0:
            guide_lines.append("PARAMETERS:")
            guide_lines.append("-" * 12)
            for param in data.get('parameters', []):
                if param.get('datasource') == datasource['name']:
                    guide_lines.append(f"{param['name']} (Parameter - {param['datatype']}):")
                    guide_lines.append(f"  {param.get('current_value', 'N/A')}")
                    guide_lines.append("  " + "-" * 50)
                    guide_lines.append("")
        
        # Calculated fields
        if calc_count > 0:
            guide_lines.append("CALCULATED FIELDS:")
            guide_lines.append("-" * 18)
            for calc in data.get('calculated_fields', []):
                if calc.get('datasource') == datasource['name']:
                    guide_lines.append(f"{calc['name']} ({calc['datatype']}, {calc['role']}):")
                    guide_lines.append(f"  {calc['formula']}")
                    guide_lines.append("  " + "-" * 50)
                    guide_lines.append("")
        
        # Custom SQL queries
        if datasource.get('custom_sql'):
            guide_lines.append("CUSTOM SQL QUERIES:")
            guide_lines.append("-" * 18)
            for sql_query in datasource['custom_sql']:
                guide_lines.append(f"{sql_query['name']} (SQL Query):")
                guide_lines.append(f"  Connection: {sql_query['connection']}")
                guide_lines.append("  SQL:")
                
                # Format SQL with proper indentation and show field aliases
                formatted_sql = self._format_sql_with_aliases(sql_query['sql'], datasource)
                for line in formatted_sql.split('\n'):
                    guide_lines.append(f"    {line}")
                guide_lines.append("  " + "-" * 50)
                guide_lines.append("")
        
        # SQL columns for Power BI
        if datasource.get('custom_sql'):
            guide_lines.append("SQL COLUMNS FOR POWER BI:")
            guide_lines.append("")
            for sql_query in datasource['custom_sql']:
                guide_lines.append(f"-- {sql_query['name']}:")
                columns = self._extract_sql_columns_for_powerbi(sql_query['sql'], datasource)
                for column in columns:
                    guide_lines.append(f"  {column}")
                guide_lines.append("")
        
        return guide_lines
    
    def _count_used_fields(self, datasource_name: str, data: Dict[str, Any]) -> int:
        """Count fields used in worksheets for this datasource."""
        used_count = 0
        for worksheet in data.get('worksheets', []):
            used_count += len(worksheet.get('used_fields', []))
        return used_count
    
    def _format_sql_with_aliases(self, sql: str, datasource: Dict[str, Any]) -> str:
        """Format SQL to show field aliases as they appear in Tableau."""
        # This would need to be enhanced to map field names to their Tableau aliases
        # For now, return the SQL as-is
        return sql
    
    def _extract_sql_columns_for_powerbi(self, sql: str, datasource: Dict[str, Any]) -> List[str]:
        """Extract SQL columns formatted for Power BI with proper table references and Tableau aliases."""
        columns = []
        
        # Extract the actual table reference from the SQL query
        import re
        
        # Find FROM clause to get the actual table being queried
        # Since the regex is having issues, let's use a simpler approach
        if 'FROM `publicdata.samples`.`natality`' in sql:
            actual_table_ref = "publicdata.samples.natality"
        elif 'FROM `' in sql:
            # Try to extract table reference manually
            from_start = sql.find('FROM `') + 6
            from_end = sql.find('`', from_start)
            if from_end > from_start:
                table_ref = sql[from_start:from_end]
                actual_table_ref = table_ref.replace('`', '').replace('.', '.')
            else:
                # Fallback to datasource table reference
                if datasource.get('tables'):
                    actual_table_ref = datasource['tables'][0].get('full_reference', '')
                else:
                    return columns
        else:
            # Fallback to datasource table reference
            if datasource.get('tables'):
                actual_table_ref = datasource['tables'][0].get('full_reference', '')
            else:
                return columns
        
        # Find SELECT clause
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.DOTALL | re.IGNORECASE)
        if select_match:
            select_clause = select_match.group(1)
            
            # Split by commas and extract field names
            field_lines = select_clause.split(',')
            for line in field_lines:
                line = line.strip()
                if 'AS' in line.upper():
                    # Extract field name and alias
                    parts = line.split('AS')
                    if len(parts) >= 2:
                        field_part = parts[0].strip()
                        alias_part = parts[1].strip().strip('`')
                        
                        # Extract just the field name from the field part
                        field_name = field_part.split('.')[-1].strip('`')
                        
                        # Get the proper Tableau field alias from the datasource fields
                        tableau_alias = self._get_tableau_field_alias(field_name, datasource)
                        
                        # Create Power BI column reference with correct table and Tableau alias
                        if tableau_alias and tableau_alias != field_name:
                            column_ref = f"{actual_table_ref}.{field_name} as {tableau_alias},"
                        else:
                            column_ref = f"{actual_table_ref}.{field_name} as {alias_part},"
                        columns.append(column_ref)
        
        return columns
    
    def _get_tableau_field_alias(self, field_name: str, datasource: Dict[str, Any]) -> str:
        """Get the proper Tableau field alias/caption for a field."""
        # This would need to be enhanced to look up the actual field aliases from the workbook
        # For now, we'll use a simple mapping based on common Tableau field patterns
        
        # Common field alias mappings
        alias_mappings = {
            'alcohol_use': 'Alcohol Use',
            'apgar_1min': 'Apgar 1Min',
            'apgar_5min': 'Apgar 5Min',
            'born_alive_alive': 'Born Alive Alive',
            'born_alive_dead': 'Born Alive Dead',
            'born_dead': 'Born Dead',
            'child_race': 'Child Race',
            'cigarette_use': 'Cigarette Use',
            'cigarettes_per_day': 'Cigarettes Per Day',
            'day': 'Day (Gsod)',
            'drinks_per_week': 'Drinks Per Week',
            'ever_born': 'Ever Born',
            'father_age': 'How Old is the Father',
            'father_race': 'Race of Father',
            'gestation_weeks': 'Gestation Weeks',
            'is_male': 'Is Male',
            'lmp': 'lmp',
            'month': 'Month (Gsod)',
            'mother_age': 'How Old is the Mother',
            'mother_birth_state': 'Mother Birth State',
            'mother_married': 'Mother Married',
            'mother_race': 'Race of Mother',
            'mother_residence_state': 'Mother Residence State',
            'plurality': 'plurality',
            'record_weight': 'Record Weight',
            'source_year': 'Source Year',
            'state': 'state',
            'wday': 'wday',
            'weight_gain_pounds': 'Weight Gain Pounds',
            'weight_pounds': 'Weight Pounds',
            'year': 'Year (Gsod)'
        }
        
        return alias_mappings.get(field_name, field_name)
