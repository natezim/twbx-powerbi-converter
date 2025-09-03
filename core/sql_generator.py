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
        """Extract SQL - with connection-type-aware processing."""
        sql_queries = []
        
        if not self.xml_root:
            return sql_queries
            
        # Find the specific datasource
        for ds in self.xml_root.findall('.//datasource'):
            if ds.get('name') == datasource_name:
                
                # STEP 1: Identify all connection types in this datasource
                connection_types = {}
                
                # Check named-connections for connection types
                for named_conn in ds.findall('.//named-connection'):
                    conn_name = named_conn.get('name', '')
                    for conn in named_conn.findall('.//connection[@class]'):
                        conn_class = conn.get('class', '')
                        connection_types[conn_name] = conn_class
                        print(f"      Found connection: {conn_name} -> {conn_class}")
                        
                        # Also map partial connection names (bigquery connections often have partial matches)
                        if 'bigquery' in conn_name.lower():
                            # Map any connection starting with the base name
                            base_name = conn_name.split('.')[0] if '.' in conn_name else conn_name
                            connection_types[base_name] = conn_class
                            print(f"      Mapped base connection: {base_name} -> {conn_class}")
                
                # Check direct connections
                for conn in ds.findall('.//connection[@class]'):
                    conn_class = conn.get('class', '')
                    if conn_class not in connection_types.values():
                        connection_types['direct'] = conn_class
                        print(f"      Found direct connection: {conn_class}")
                
                # STEP 2: Process ONLY text relations (actual custom SQL) - skip joins and table references
                text_relations = [r for r in ds.findall('.//relation') if r.get('type') == 'text' and r.text and r.text.strip()]
                print(f"      Found {len(text_relations)} text relations (custom SQL)")
                
                for relation in text_relations:
                    connection_name = relation.get('connection', '')
                    query_name = relation.get('name', 'Custom Query')
                    
                    # Determine the connection class for this relation
                    conn_class = connection_types.get(connection_name, 'unknown')
                    
                    # If we didn't find exact match, try fuzzy matching for BigQuery
                    if conn_class == 'unknown' and connection_name:
                        for conn_key, conn_value in connection_types.items():
                            if ('bigquery' in conn_key.lower() and 'bigquery' in connection_name.lower()) or \
                               (conn_key in connection_name or connection_name in conn_key):
                                conn_class = conn_value
                                print(f"      Fuzzy matched: {connection_name} -> {conn_key} ({conn_class})")
                                break
                    
                    print(f"      Processing SQL: {query_name} (conn={connection_name}, class={conn_class})")
                    
                    # Clean up SQL (remove << >> artifacts from Tableau)
                    sql_text = relation.text.strip()
                    sql_text = sql_text.replace('<<', '<').replace('>>', '>')
                    sql_text = re.sub(r'\r\n', r'\n', sql_text)  # Normalize newlines
                    
                    # This is custom SQL - the real queries we want!
                    sql_type = self._get_sql_type_for_connection(conn_class, 'Custom SQL')
                    sql_queries.append({
                        'name': query_name,
                        'sql': sql_text,
                        'type': sql_type,
                        'connection': connection_name,
                        'connection_class': conn_class
                    })
                
                # STEP 2b: Extract join information separately (but don't treat as SQL queries)
                join_relations = [r for r in ds.findall('.//relation') if r.get('type') == 'join']
                if join_relations:
                    print(f"      Found {len(join_relations)} join relations (for documentation)")
                    join_info = self._extract_joins_for_datasource(ds)
                    if join_info:
                        sql_queries.append({
                            'name': 'Join Information',
                            'sql': join_info,
                            'type': 'Join Documentation',
                            'connection': '',
                            'connection_class': 'documentation'
                        })
                
                # STEP 3: Extract connection information for documentation
                for named_conn in ds.findall('.//named-connection'):
                    conn_name = named_conn.get('name', '')
                    caption = named_conn.get('caption', 'Connection')
                    
                    for conn in named_conn.findall('.//connection[@class]'):
                        conn_class = conn.get('class', '')
                        conn_info = self._extract_connection_info(conn, conn_class, caption)
                        if conn_info:
                            sql_type = self._get_sql_type_for_connection(conn_class, 'Connection Info')
                            sql_queries.append({
                                'name': f'{caption} Connection',
                                'sql': conn_info,
                                'type': sql_type,
                                'connection': conn_name,
                                'connection_class': conn_class
                            })
        
        return sql_queries
    
    def _get_sql_type_for_connection(self, conn_class, base_type):
        """Get the appropriate SQL type based on connection class."""
        if conn_class == 'bigquery':
            return f'BigQuery {base_type}'
        elif conn_class == 'postgres':
            return f'PostgreSQL {base_type}'
        elif conn_class == 'sqlserver':
            return f'SQL Server {base_type}'
        elif conn_class == 'mysql':
            return f'MySQL {base_type}'
        elif conn_class == 'oracle':
            return f'Oracle {base_type}'
        elif conn_class == 'snowflake':
            return f'Snowflake {base_type}'
        else:
            return f'{conn_class.title()} {base_type}' if conn_class != 'unknown' else base_type
    
    def _extract_join_from_relation(self, relation, conn_class):
        """Extract join information from a join relation."""
        join_info = []
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
                        left_clean = self.clean_field_reference(left_field)
                        right_clean = self.clean_field_reference(right_field)
                        join_condition = f"-- JOIN ON: {left_clean} = {right_clean}"
                        join_info.append(join_condition)
        
        return '\n'.join(join_info) if join_info else None
    
    def _generate_table_sql(self, table_name, conn_class):
        """Generate appropriate SQL for table reference based on connection class."""
        if conn_class == 'bigquery':
            # BigQuery uses backticks for table names
            return f'SELECT * FROM `{table_name}`;'
        elif conn_class in ['postgres', 'mysql']:
            # PostgreSQL and MySQL can use double quotes for identifiers
            return f'SELECT * FROM "{table_name}";'
        elif conn_class == 'sqlserver':
            # SQL Server uses square brackets
            return f'SELECT * FROM [{table_name}];'
        else:
            # Default format
            return f'SELECT * FROM {table_name};'
    
    def _extract_connection_info(self, conn, conn_class, caption):
        """Extract connection information based on connection class."""
        if conn_class == 'bigquery':
            project = conn.get('project', '') or conn.get('CATALOG', '') or conn.get('EXECCATALOG', '')
            schema = conn.get('schema', '')
            
            if project:
                info = f'-- BigQuery Connection: {caption}\n'
                info += f'-- Project: {project}\n'
                if schema:
                    info += f'-- Schema: {schema}\n'
                info += '-- Use BigQuery connector in Power BI'
                return info
                
        elif conn_class == 'postgres':
            server = conn.get('server', '')
            dbname = conn.get('dbname', '')
            port = conn.get('port', '5432')
            
            if server:
                info = f'-- PostgreSQL Connection: {caption}\n'
                info += f'-- Server: {server}\n'
                info += f'-- Database: {dbname}\n'
                info += f'-- Port: {port}\n'
                info += '-- Use PostgreSQL connector in Power BI'
                return info
                
        elif conn_class == 'sqlserver':
            server = conn.get('server', '')
            dbname = conn.get('dbname', '')
            
            if server:
                info = f'-- SQL Server Connection: {caption}\n'
                info += f'-- Server: {server}\n'
                info += f'-- Database: {dbname}\n'
                info += '-- Use SQL Server connector in Power BI'
                return info
        
        # Default connection info
        server = conn.get('server', '')
        dbname = conn.get('dbname', '')
        if server or dbname:
            info = f'-- {conn_class.title()} Connection: {caption}\n'
            if server:
                info += f'-- Server: {server}\n'
            if dbname:
                info += f'-- Database: {dbname}\n'
            return info
        
        return None
    
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
    
    def _extract_joins_for_datasource(self, datasource_xml):
        """Extract join information for the entire datasource."""
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
                            left_clean = self.clean_field_reference(left_field)
                            right_clean = self.clean_field_reference(right_field)
                            join_condition = f"-- JOIN ON: {left_clean} = {right_clean}"
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