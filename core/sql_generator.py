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
    
    def extract_sql_from_xml(self, datasource_name):
        """Extract SQL - with BigQuery support."""
        sql_queries = []
        
        if not self.xml_root:
            return sql_queries
            
        # Find the specific datasource
        for ds in self.xml_root.findall('.//datasource'):
            if ds.get('name') == datasource_name:
                
                # Method 1: Extract custom SQL queries from relation[@type="text"] elements
                for relation in ds.findall('.//relation[@type="text"]'):
                    if relation.text and relation.text.strip():
                        connection_name = relation.get('connection', '')
                        query_name = relation.get('name', 'Custom Query')
                        
                        # Check if this is a BigQuery connection
                        is_bigquery = 'bigquery' in connection_name.lower()
                        query_type = 'BigQuery Custom SQL' if is_bigquery else 'Custom SQL'
                        
                        sql_queries.append({
                            'name': query_name,
                            'sql': relation.text.strip(),
                            'type': query_type,
                            'connection': connection_name
                        })
                
                # Method 2: Extract BigQuery connection info from named-connections
                for named_conn in ds.findall('.//named-connection'):
                    bg_conn = named_conn.find('.//connection[@class="bigquery"]')
                    if bg_conn is not None:
                        # Extract BigQuery project and schema info
                        project = bg_conn.get('project', '') or bg_conn.get('CATALOG', '') or bg_conn.get('EXECCATALOG', '')
                        schema = bg_conn.get('schema', '')
                        connection_name = named_conn.get('name', '')
                        caption = named_conn.get('caption', 'BigQuery Connection')
                        
                        if project:
                            connection_info = f'-- BigQuery Connection: {caption}\n'
                            connection_info += f'-- Project: {project}\n'
                            if schema:
                                connection_info += f'-- Schema: {schema}\n'
                            connection_info += f'-- Connection ID: {connection_name}\n'
                            connection_info += '-- Use BigQuery connector in Power BI'
                            
                            sql_queries.append({
                                'name': f'BigQuery Connection: {caption}',
                                'sql': connection_info,
                                'type': 'BigQuery Connection Info',
                                'connection': connection_name,
                                'project': project,
                                'schema': schema
                            })
                
                # Method 3: Extract join information for BigQuery datasources
                if 'bigquery' in ds.get('caption', '').lower():
                    join_info = self.extract_bigquery_joins(ds)
                    if join_info:
                        sql_queries.append({
                            'name': 'BigQuery Join Information',
                            'sql': join_info,
                            'type': 'BigQuery Join Info'
                        })
                
                # Method 4: Look for relation with BigQuery table references
                for connection in ds.findall('.//connection[@class="bigquery"]'):
                    for relation in connection.findall('.//relation'):
                        table_ref = relation.get('table', '')
                        if table_ref:
                            # BigQuery table format: project.dataset.table
                            sql_queries.append({
                                'name': f'BigQuery Table: {table_ref}',
                                'sql': f'SELECT * FROM `{table_ref}`;',
                                'type': 'BigQuery Table Reference'
                            })
                
                # Method 5: Look for metadata-record with BigQuery SQL
                for connection in ds.findall('.//connection[@class="bigquery"]'):
                    for metadata in connection.findall('.//metadata-record'):
                        if metadata.get('class') == 'relation':
                            local_name = metadata.find('local-name')
                            remote_name = metadata.find('remote-name')
                            if remote_name is not None and remote_name.text:
                                if '.' in remote_name.text:  # BigQuery format
                                    sql_queries.append({
                                        'name': f'BigQuery Dataset: {local_name.text if local_name is not None else "Unknown"}',
                                        'sql': f'SELECT * FROM `{remote_name.text}`;',
                                        'type': 'BigQuery Dataset Reference'
                                    })
        
        return sql_queries
    
    def extract_bigquery_joins(self, datasource_xml):
        """Extract join information specifically for BigQuery datasources."""
        join_info = []
        
        # Look for join relations
        for relation in datasource_xml.findall('.//relation[@type="join"]'):
            join_type = relation.get('join', 'inner')
            join_info.append(f"-- {join_type.upper()} JOIN detected")
            
            # Extract join conditions
            for clause in relation.findall('.//clause[@type="join"]'):
                for expr in clause.findall('.//expression[@op="="]'):
                    nested_exprs = expr.findall('.//expression[@op]')
                    if len(nested_exprs) >= 2:
                        left_field = nested_exprs[0].get('op', '')
                        right_field = nested_exprs[1].get('op', '')
                        
                        if left_field and right_field:
                            # Clean the field references
                            left_clean = self.clean_field_reference(left_field)
                            right_clean = self.clean_field_reference(right_field)
                            join_condition = f"-- JOIN ON: {left_clean} = {right_clean}"
                            join_info.append(join_condition)
        
        # Look for standalone join clauses
        for clause in datasource_xml.findall('.//clause[@type="join"]'):
            for expr in clause.findall('.//expression[@op="="]'):
                nested_exprs = expr.findall('.//expression[@op]')
                if len(nested_exprs) >= 2:
                    left_field = nested_exprs[0].get('op', '')
                    right_field = nested_exprs[1].get('op', '')
                    
                    if left_field and right_field:
                        left_clean = self.clean_field_reference(left_field)
                        right_clean = self.clean_field_reference(right_field)
                        join_condition = f"-- JOIN CONDITION: {left_clean} = {right_clean}"
                        if join_condition not in join_info:
                            join_info.append(join_condition)
        
        return '\n'.join(join_info) if join_info else None
    
    def debug_bigquery_structure(self, datasource_name):
        """Debug method to see BigQuery XML structure."""
        if not self.xml_root:
            return
        
        for ds in self.xml_root.findall('.//datasource'):
            if ds.get('name') == datasource_name:
                print(f"Debugging datasource: {datasource_name}")
                print(f"Datasource caption: {ds.get('caption', 'N/A')}")
                
                # Look for federated connections
                federated_conns = ds.findall('.//connection[@class="federated"]')
                print(f"Found {len(federated_conns)} federated connections")
                
                # Look for named connections that might contain BigQuery
                named_conns = ds.findall('.//named-connection')
                print(f"Found {len(named_conns)} named connections")
                
                for i, named_conn in enumerate(named_conns):
                    caption = named_conn.get('caption', 'unnamed')
                    name = named_conn.get('name', 'unnamed')
                    print(f"Named connection {i}: '{caption}' (name: {name})")
                    
                    bg_nested = named_conn.find('.//connection[@class="bigquery"]')
                    if bg_nested is not None:
                        print(f"  ✓ Contains BigQuery connection!")
                        print(f"  Attributes: {bg_nested.attrib}")
                        
                        # Extract key BigQuery properties
                        project = bg_nested.get('project', '') or bg_nested.get('CATALOG', '') or bg_nested.get('EXECCATALOG', '')
                        schema = bg_nested.get('schema', '')
                        print(f"  Project: {project}")
                        print(f"  Schema: {schema}")
                
                # Look for custom SQL in relation[@type="text"] elements
                text_relations = ds.findall('.//relation[@type="text"]')
                print(f"Found {len(text_relations)} text relations (custom SQL)")
                
                for i, rel in enumerate(text_relations):
                    connection = rel.get('connection', '')
                    name = rel.get('name', '')
                    sql_content = rel.text.strip() if rel.text else ''
                    
                    print(f"Text relation {i}:")
                    print(f"  Name: {name}")
                    print(f"  Connection: {connection}")
                    print(f"  Is BigQuery: {'bigquery' in connection.lower()}")
                    if sql_content:
                        print(f"  SQL: {sql_content[:100]}{'...' if len(sql_content) > 100 else ''}")
                
                # Look for join information
                join_relations = ds.findall('.//relation[@type="join"]')
                print(f"Found {len(join_relations)} join relations")
                
                for i, join_rel in enumerate(join_relations):
                    join_type = join_rel.get('join', 'inner')
                    print(f"Join relation {i}: {join_type} join")
                
                # Look for join clauses
                join_clauses = ds.findall('.//clause[@type="join"]')
                print(f"Found {len(join_clauses)} join clauses")
                
                for i, clause in enumerate(join_clauses):
                    print(f"Join clause {i}:")
                    exprs = clause.findall('.//expression[@op="="]')
                    for j, expr in enumerate(exprs):
                        nested_exprs = expr.findall('.//expression[@op]')
                        if len(nested_exprs) >= 2:
                            left = nested_exprs[0].get('op', '')
                            right = nested_exprs[1].get('op', '')
                            print(f"    {left} = {right}")
                
                # Look for any BigQuery-specific elements
                bg_elements = ds.findall('.//connection[@class="bigquery"]')
                print(f"Found {len(bg_elements)} direct BigQuery connections")
                
                for i, bg_conn in enumerate(bg_elements):
                    print(f"Direct BigQuery connection {i}:")
                    print(f"  Attributes: {bg_conn.attrib}")
                
                # Look for all relation elements
                all_relations = ds.findall('.//relation')
                print(f"Found {len(all_relations)} total relations")
                
                for i, rel in enumerate(all_relations):
                    rel_type = rel.get('type', 'unknown')
                    rel_name = rel.get('name', 'unnamed')
                    connection = rel.get('connection', '')
                    table = rel.get('table', '')
                    
                    is_bigquery_related = ('bigquery' in connection.lower() or 
                                         'bigquery' in rel_name.lower() or
                                         rel_type == 'text')
                    
                    if is_bigquery_related:
                        print(f"  Relation {i} (BigQuery-related): type={rel_type}, name={rel_name}")
                        if connection:
                            print(f"    Connection: {connection}")
                        if table:
                            print(f"    Table: {table}")
                        if rel.text and rel.text.strip():
                            print(f"    Content: {rel.text.strip()[:50]}...")
    
    def enhance_bigquery_sql_extraction(self, datasource_name):
        """Enhanced BigQuery SQL extraction with debug output."""
        print(f"\n=== Enhanced BigQuery Extraction for: {datasource_name} ===")
        
        # First run debug to see structure
        self.debug_bigquery_structure(datasource_name)
        
        # Then extract SQL with BigQuery support
        sql_queries = self.extract_sql_from_xml(datasource_name)
        
        print(f"\nExtracted {len(sql_queries)} SQL queries:")
        for i, query in enumerate(sql_queries):
            print(f"  {i+1}. {query['name']} ({query['type']})")
            if len(query['sql']) > 100:
                print(f"     SQL: {query['sql'][:100]}...")
            else:
                print(f"     SQL: {query['sql']}")
        
        return sql_queries