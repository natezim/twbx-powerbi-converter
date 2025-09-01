#!/usr/bin/env python3
"""
SQL Generator Module
Handles SQL extraction and relationship mapping from Tableau XML
"""

import xml.etree.ElementTree as ET
import re


class SQLGenerator:
    """Extracts SQL information and generates migration SQL."""
    
    def __init__(self, xml_root):
        self.xml_root = xml_root
    
    def extract_sql_from_tableau_xml(self, datasource_xml):
        """Extract all SQL-related information from Tableau XML using only standard library."""
        sql_info = {
            'custom_sql': [],
            'table_references': [],
            'relationships': [],
            'all_tables': {},
            'join_conditions': []
        }
        
        # 1. CUSTOM SQL (Priority #1 - this is usually what teams need most)
        for relation in datasource_xml.findall('.//relation[@type="text"]'):
            if relation.text and relation.text.strip():
                sql_info['custom_sql'].append({
                    'name': relation.get('name', 'Custom Query'),
                    'sql': relation.text.strip(),
                    'connection': relation.get('connection', '')
                })
        
        # 2. EXTRACT ACTUAL DATABASE TABLE NAMES (not Tableau aliases)
        for relation in datasource_xml.findall('.//relation[@table]'):
            table_name = relation.get('table', '')
            table_alias = relation.get('name', '')
            connection = relation.get('connection', '')
            
            if table_name:
                # Clean up table name - remove [public]. prefix
                clean_table = table_name.replace('[public].', '').replace('[', '').replace(']', '')
                
                sql_info['table_references'].append({
                    'table': clean_table,
                    'alias': table_alias,
                    'connection': connection,
                    'type': relation.get('type', 'table')
                })
                
                # Store the REAL table name, not the alias
                if table_alias:
                    sql_info['all_tables'][table_alias] = {
                        'table_name': clean_table,  # This is the actual database table
                        'connection': connection
                    }
        
        # 3. EXTRACT JOIN RELATIONSHIPS - FIXED VERSION
        # Look for join relationships in the XML
        for relation in datasource_xml.findall('.//relation[@type="join"]'):
            # Extract join conditions from nested expressions
            join_conditions = []
            
            # Look for expressions with op='=' (join operators)
            for expr in relation.findall('.//expression[@op="="]'):
                # Get the nested expressions that contain the actual field references
                nested_exprs = expr.findall('.//expression[@op]')
                if len(nested_exprs) >= 2:
                    # Extract the field references from the op attributes
                    left_field = nested_exprs[0].get('op', '')
                    right_field = nested_exprs[1].get('op', '')
                    
                    if left_field and right_field:
                        # Clean the field references and create proper join condition
                        left_clean = self.clean_field_reference(left_field)
                        right_clean = self.clean_field_reference(right_field)
                        join_condition = f"{left_clean} = {right_clean}"
                        join_conditions.append(join_condition)
            
            # Store relationship info
            if join_conditions:
                join_type = relation.get('join', 'left')
                sql_info['relationships'].append({
                    'join_type': join_type,
                    'conditions': join_conditions,
                    'tables': []
                })
                
                # Extract tables involved in this join
                for table_rel in relation.findall('.//relation[@table]'):
                    table_name = table_rel.get('table', '').replace('[public].', '').replace('[', '').replace(']', '')
                    table_alias = table_rel.get('name', '')
                    if table_name and table_alias:
                        sql_info['relationships'][-1]['tables'].append({
                            'alias': table_alias,
                            'table_name': table_name
                        })
        
        # 4. EXTRACT ADDITIONAL JOIN CONDITIONS from other parts of XML
        for clause in datasource_xml.findall('.//clause[@type="join"]'):
            for expr in clause.findall('.//expression[@op="="]'):
                nested_exprs = expr.findall('.//expression[@op]')
                if len(nested_exprs) >= 2:
                    left_field = nested_exprs[0].get('op', '')
                    right_field = nested_exprs[1].get('op', '')
                    
                    if left_field and right_field:
                        # Clean the field references
                        left_clean = self.clean_field_reference(left_field)
                        right_clean = self.clean_field_reference(right_field)
                        join_condition = f"{left_clean} = {right_clean}"
                        sql_info['join_conditions'].append(join_condition)
        
        return sql_info
    
    # SQL generation methods removed - now using text setup guides instead
    
    def clean_join_condition(self, join_condition):
        """Clean join condition for universal database compatibility."""
        # Remove brackets from field references
        clean_condition = join_condition.replace('[', '').replace(']', '')
        
        # Replace spaces with underscores in field names
        clean_condition = clean_condition.replace(' ', '_')
        
        return clean_condition
    
    def clean_table_name(self, table_name):
        """Clean table name for universal database compatibility."""
        # Remove brackets
        clean_name = table_name.replace('[', '').replace(']', '')
        
        # Replace spaces with underscores
        clean_name = clean_name.replace(' ', '_')
        
        return clean_name
    
    def clean_field_reference(self, field_ref):
        """Clean field reference for proper SQL formatting."""
        # First: replace spaces with underscores INSIDE brackets
        # This handles cases like [Away Teams].[team_abbr] -> [Away_Teams].[team_abbr]
        clean_ref = re.sub(r'\[([^\]]*)\]', lambda m: '[' + m.group(1).replace(' ', '_') + ']', field_ref)
        
        # Second: remove brackets
        clean_ref = clean_ref.replace('[', '').replace(']', '')
        
        return clean_ref
