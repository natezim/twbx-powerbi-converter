#!/usr/bin/env python3
"""
Enhanced Tableau Migrator
Integrates comprehensive data extraction with existing functionality
"""

import os
import json
from datetime import datetime
from typing import Dict, List, Any
from .tableau_parser import TableauParser
from .comprehensive_extractor import ComprehensiveTableauExtractor
from .field_definitions_extractor import FieldDefinitionsExtractor
from .field_extractor import FieldExtractor
from .sql_generator import SQLGenerator
from .csv_exporter import CSVExporter
from .thumbnail_extractor import ThumbnailExtractor
from utils.file_utils import ensure_directory_exists


class EnhancedTableauMigrator:
    """Enhanced migrator with comprehensive data extraction capabilities."""
    
    def __init__(self):
        self.parser = None
        self.comprehensive_extractor = None
        self.field_definitions_extractor = None
        self.field_extractor = None
        self.sql_generator = None
        self.csv_exporter = CSVExporter()
        self.thumbnail_extractor = ThumbnailExtractor()
    
    def process_tableau_file_comprehensive(self, tableau_path: str, output_format: str = "all") -> Dict:
        """
        Process a Tableau file with comprehensive data extraction.
        
        Args:
            tableau_path: Path to the Tableau file
            output_format: Output format - "all", "json", "csv", "legacy"
        
        Returns:
            Dictionary containing all extracted data
        """
        print(f"🔄 Processing with comprehensive extraction: {tableau_path}")
        print("-" * 70)
        
        try:
            # 1. Parse Tableau file
            self.parser = TableauParser(tableau_path)
            if not self.parser.extract_and_parse():
                print(f"❌ Failed to parse {tableau_path}")
                return None
            
            file_type = "TWBX" if tableau_path.endswith('.twbx') else "TWB"
            print(f"✅ {file_type} parsed successfully using official Tableau API + XML")
            
            # 2. Initialize comprehensive extractor
            self.comprehensive_extractor = ComprehensiveTableauExtractor(
                self.parser.get_xml_root(), 
                self.parser.get_workbook(),
                tableau_path
            )
            
            # 3. Initialize field definitions extractor for universal troubleshooting
            self.field_definitions_extractor = FieldDefinitionsExtractor()
            
            # 4. Extract comprehensive data
            print("🔍 Extracting comprehensive data...")
            comprehensive_data = self.comprehensive_extractor.extract_all_data()
            
            # 5. Extract universal field definitions
            print("🔍 Extracting universal field definitions...")
            field_definitions = self.field_definitions_extractor.extract_all_field_definitions(
                self.parser.get_workbook(),
                self.parser.get_xml_root()
            )
            
            # 6. Extract thumbnails
            print("🖼️ Extracting thumbnails...")
            self._extract_thumbnails(tableau_path)
            
            # 7. Export based on requested format
            if output_format in ["all", "json"]:
                self._export_comprehensive_json(comprehensive_data, tableau_path, field_definitions)
            

            
            print("✅ Comprehensive processing complete!")
            return comprehensive_data
            
        except Exception as e:
            print(f"❌ Error in comprehensive processing: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _export_comprehensive_json(self, data: Dict, tableau_path: str, field_definitions: Dict = None) -> None:
        """Export comprehensive data to a single consolidated JSON file."""
        # Create output directory
        workbook_name = os.path.splitext(os.path.basename(tableau_path))[0]
        safe_workbook = self._create_safe_filename(workbook_name)
        output_dir = os.path.join('output', safe_workbook)
        ensure_directory_exists(output_dir)
        
        # Create consolidated data structure
        consolidated_data = {
            "workbook_info": {
                "name": safe_workbook,
                "original_filename": os.path.basename(tableau_path),
                "extraction_timestamp": data.get("extraction_timestamp", ""),
                "total_datasources": data.get("workbook_metadata", {}).get("total_datasources", 0),
                "total_worksheets": data.get("workbook_metadata", {}).get("total_worksheets", 0),
                "total_dashboards": data.get("workbook_metadata", {}).get("total_dashboards", 0),
                "complexity_score": data.get("workbook_metadata", {}).get("complexity_score", "Unknown")
            },
            "workbook_metadata": data.get("workbook_metadata", {}),
            "datasources": data.get("datasources", []),
            "fields_comprehensive": data.get("fields_comprehensive", {}),
            "worksheets": data.get("worksheets", []),
            "dashboards": data.get("dashboards", []),
            "parameters": data.get("parameters", []),
            "calculated_fields": data.get("calculated_fields", []),
            "powerbi_migration_guide": data.get("powerbi_migration_guide", {}),
            "field_definitions_universal": field_definitions or {}
        }
        
        # Export single comprehensive file
        comprehensive_path = os.path.join(output_dir, f"{safe_workbook}_complete.json")
        with open(comprehensive_path, 'w', encoding='utf-8') as f:
            json.dump(consolidated_data, f, indent=2, ensure_ascii=False)
        print(f"✅ Complete data exported to: {os.path.basename(comprehensive_path)}")
    
    def _extract_thumbnails(self, tableau_path: str) -> None:
        """Extract thumbnail screenshots from the Tableau file."""
        try:
            from core.thumbnail_extractor import ThumbnailExtractor
            
            # Create output directory
            workbook_name = os.path.splitext(os.path.basename(tableau_path))[0]
            safe_workbook = self._create_safe_filename(workbook_name)
            output_dir = os.path.join('output', safe_workbook)
            ensure_directory_exists(output_dir)
            
            # Extract thumbnails using the XML root from the parser
            thumbnail_extractor = ThumbnailExtractor()
            xml_root = self.parser.get_xml_root()
            
            thumbnails = thumbnail_extractor.extract_thumbnails(xml_root, output_dir)
            print(f"✅ Extracted {thumbnails.get('extracted_count', 0)} thumbnail(s)")
            
        except Exception as e:
            print(f"⚠️ Warning: Could not extract thumbnails: {e}")
            # Don't fail the entire process if thumbnails fail
    
    def _export_legacy_formats(self, data: Dict, tableau_path: str) -> None:
        """Export data in legacy CSV and text formats."""
        # Convert comprehensive data to legacy format for existing exporters
        legacy_data_sources = self._convert_to_legacy_format(data)
        
        # Create output directory
        workbook_name = os.path.splitext(os.path.basename(tableau_path))[0]
        safe_workbook = self._create_safe_filename(workbook_name)
        output_dir = os.path.join('output', safe_workbook)
        ensure_directory_exists(output_dir)
        
        # Export using existing CSV exporter
        print("💾 Exporting legacy formats...")
        self.csv_exporter.export_setup_guide_txt(output_dir, legacy_data_sources)
        self.csv_exporter.export_field_mapping_csv(output_dir, legacy_data_sources)
        
        # Export dashboard usage if available
        if data.get('worksheets') or data.get('dashboards'):
            dashboard_info = self._extract_dashboard_info_for_legacy(data)
            self.csv_exporter.export_dashboard_usage_csv(output_dir, legacy_data_sources, dashboard_info)
        
        # Extract thumbnails
        print("🖼️  Extracting thumbnails...")
        self._extract_thumbnails_legacy(data, output_dir)
    
    def _convert_to_legacy_format(self, comprehensive_data: Dict) -> List[Dict]:
        """Convert comprehensive data to legacy format for existing exporters."""
        legacy_data_sources = []
        
        # Convert datasources
        for ds in comprehensive_data.get('datasources', []):
            legacy_ds = {
                'name': ds['name'],
                'caption': ds['caption'],
                'workbook_name': comprehensive_data['workbook_metadata']['name'],
                'twbx_path': '',  # Will be set by caller
                'connections': ds['connections'],
                'fields': [],
                'field_count': ds['field_count'],
                'sql_info': {
                    'custom_sql': ds['custom_sql'],
                    'table_references': ds['tables'],
                    'relationships': ds['relationships']
                }
            }
            
            # Convert fields
            all_fields = (
                comprehensive_data['fields_comprehensive']['regular_fields'] +
                comprehensive_data['fields_comprehensive']['calculated_fields'] +
                comprehensive_data['fields_comprehensive']['parameters']
            )
            
            for field in all_fields:
                legacy_field = {
                    'name': field['name'],
                    'caption': field['caption'],
                    'datatype': field['datatype'],
                    'role': field['role'],
                    'id': field.get('id', field['name']),
                    'table_name': 'Unknown',
                    'remote_name': field['name'],
                    'table_reference': 'Unknown',
                    'used_in_workbook': len(field.get('used_in_worksheets', [])) > 0,
                    'is_calculated': field.get('is_calculated', False),
                    'is_parameter': field.get('is_parameter', False),
                    'calculation_formula': field.get('calculation_formula', ''),
                    'calculation_class': 'tableau'
                }
                legacy_ds['fields'].append(legacy_field)
            
            legacy_data_sources.append(legacy_ds)
        
        return legacy_data_sources
    
    def _extract_dashboard_info_for_legacy(self, data: Dict) -> Dict:
        """Extract dashboard info in legacy format."""
        dashboard_info = {}
        
        # Convert worksheets
        for ws in data.get('worksheets', []):
            dashboard_info[ws['name']] = {
                'type': 'worksheet',
                'chart_type': ws['chart_type'],
                'used_fields': ws['used_fields'],
                'filters': ws['filters']
            }
        
        # Convert dashboards
        for dashboard in data.get('dashboards', []):
            dashboard_info[dashboard['name']] = {
                'type': 'dashboard',
                'width': dashboard['size']['width'],
                'height': dashboard['size']['height'],
                'included_worksheets': [obj['name'] for obj in dashboard['contained_objects'] if obj['type'] == 'worksheet'],
                'filters': dashboard['filters']
            }
        
        return dashboard_info
    
    def _extract_thumbnails_legacy(self, data: Dict, output_dir: str) -> None:
        """Extract thumbnails using legacy method."""
        if self.parser and self.parser.get_xml_root():
            try:
                results = self.thumbnail_extractor.extract_thumbnails(
                    self.parser.get_xml_root(), 
                    output_dir
                )
                
                if results['extracted_count'] > 0:
                    print(f"   ✅ Successfully extracted {results['extracted_count']} thumbnail(s)")
                    print(f"   📁 Saved to: {results['screenshots_dir']}")
                else:
                    print("   ℹ️  No thumbnails found in this workbook")
                    
            except Exception as e:
                print(f"   ❌ Thumbnail extraction failed: {str(e)}")
    
    def _create_safe_filename(self, filename: str) -> str:
        """Create a safe filename by removing/replacing special characters."""
        import re
        # Replace spaces and special characters with underscores
        safe_name = re.sub(r'[^\w\-_]', '_', filename)
        # Remove multiple consecutive underscores
        safe_name = re.sub(r'_+', '_', safe_name)
        # Remove leading/trailing underscores
        safe_name = safe_name.strip('_')
        return safe_name
    
    def generate_powerbi_migration_report(self, comprehensive_data: Dict, output_path: str) -> None:
        """Generate a detailed Power BI migration report."""
        report = {
            "migration_summary": {
                "workbook_name": comprehensive_data['workbook_metadata']['name'],
                "complexity_score": comprehensive_data['workbook_metadata']['complexity_score'],
                "total_worksheets": comprehensive_data['workbook_metadata']['total_worksheets'],
                "total_dashboards": comprehensive_data['workbook_metadata']['total_dashboards'],
                "total_datasources": comprehensive_data['workbook_metadata']['total_datasources'],
                "total_fields": comprehensive_data['fields_comprehensive']['total_fields'],
                "total_calculations": len(comprehensive_data['calculated_fields']),
                "total_parameters": len(comprehensive_data['parameters']),
                "estimated_migration_time": self._estimate_migration_time(comprehensive_data)
            },
            "migration_steps": self._generate_detailed_migration_steps(comprehensive_data),
            "data_source_analysis": self._analyze_data_sources(comprehensive_data),
            "visual_migration_plan": self._create_visual_migration_plan(comprehensive_data),
            "calculated_field_migration": self._plan_calculated_field_migration(comprehensive_data),
            "parameter_migration": self._plan_parameter_migration(comprehensive_data),
            "recommendations": self._generate_migration_recommendations(comprehensive_data)
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Power BI migration report generated: {output_path}")
    
    def _estimate_migration_time(self, data: Dict) -> str:
        """Estimate migration time based on complexity."""
        complexity = data['workbook_metadata']['complexity_score']
        worksheet_count = data['workbook_metadata']['total_worksheets']
        calculation_count = len(data['calculated_fields'])
        
        if complexity == "High":
            base_hours = 8
        elif complexity == "Medium":
            base_hours = 4
        else:
            base_hours = 2
        
        # Add time for worksheets and calculations
        worksheet_hours = worksheet_count * 0.5
        calculation_hours = calculation_count * 0.25
        
        total_hours = base_hours + worksheet_hours + calculation_hours
        
        if total_hours < 4:
            return f"{total_hours:.1f} hours"
        elif total_hours < 8:
            return f"{total_hours:.1f} hours (1 day)"
        else:
            days = total_hours / 8
            return f"{total_hours:.1f} hours ({days:.1f} days)"
    
    def _generate_detailed_migration_steps(self, data: Dict) -> List[Dict[str, Any]]:
        """Generate detailed migration steps."""
        steps = [
            {
                "step": 1,
                "title": "Data Source Setup",
                "description": "Set up data sources in Power BI",
                "estimated_time": "1-2 hours",
                "details": [
                    "Connect to data sources using Power Query",
                    "Configure authentication and connection strings",
                    "Test data connections"
                ]
            },
            {
                "step": 2,
                "title": "Data Model Creation",
                "description": "Create data model and relationships",
                "estimated_time": "2-4 hours",
                "details": [
                    "Import data tables",
                    "Create relationships between tables",
                    "Set up data types and formatting"
                ]
            },
            {
                "step": 3,
                "title": "Calculated Fields Migration",
                "description": f"Migrate {len(data['calculated_fields'])} calculated fields",
                "estimated_time": f"{len(data['calculated_fields']) * 0.25:.1f} hours",
                "details": [
                    "Convert Tableau formulas to DAX",
                    "Create measures and calculated columns",
                    "Test calculation results"
                ]
            },
            {
                "step": 4,
                "title": "Visual Creation",
                "description": f"Create {data['workbook_metadata']['total_worksheets']} visuals",
                "estimated_time": f"{data['workbook_metadata']['total_worksheets'] * 0.5:.1f} hours",
                "details": [
                    "Create visuals matching Tableau worksheets",
                    "Configure field placements and formatting",
                    "Set up filters and interactions"
                ]
            },
            {
                "step": 5,
                "title": "Dashboard Creation",
                "description": f"Create {data['workbook_metadata']['total_dashboards']} dashboard pages",
                "estimated_time": f"{data['workbook_metadata']['total_dashboards'] * 1:.1f} hours",
                "details": [
                    "Create report pages",
                    "Arrange visuals and configure layout",
                    "Set up page-level filters"
                ]
            },
            {
                "step": 6,
                "title": "Testing and Validation",
                "description": "Test and validate migration results",
                "estimated_time": "2-4 hours",
                "details": [
                    "Compare results with original Tableau workbook",
                    "Test all interactions and filters",
                    "Validate data accuracy"
                ]
            }
        ]
        
        return steps
    
    def _analyze_data_sources(self, data: Dict) -> Dict[str, Any]:
        """Analyze data sources for migration complexity."""
        analysis = {
            "total_datasources": len(data['datasources']),
            "connection_types": {},
            "complexity_analysis": {},
            "migration_notes": []
        }
        
        for ds in data['datasources']:
            for conn in ds['connections']:
                dbclass = conn.get('dbclass', 'Unknown')
                if dbclass not in analysis['connection_types']:
                    analysis['connection_types'][dbclass] = 0
                analysis['connection_types'][dbclass] += 1
        
        # Add migration notes based on connection types
        if 'bigquery' in analysis['connection_types']:
            analysis['migration_notes'].append("BigQuery connections require proper authentication setup in Power BI")
        
        if 'sqlserver' in analysis['connection_types']:
            analysis['migration_notes'].append("SQL Server connections may need gateway configuration")
        
        return analysis
    
    def _create_visual_migration_plan(self, data: Dict) -> List[Dict[str, Any]]:
        """Create a plan for migrating visuals."""
        migration_plan = []
        
        for ws in data['worksheets']:
            plan_item = {
                "worksheet_name": ws['name'],
                "chart_type": ws['chart_type'],
                "powerbi_visual_type": self._map_to_powerbi_visual(ws['chart_type']),
                "migration_difficulty": self._assess_visual_difficulty(ws),
                "required_fields": ws['used_fields'],
                "instructions": ws['powerbi_instructions']
            }
            migration_plan.append(plan_item)
        
        return migration_plan
    
    def _map_to_powerbi_visual(self, tableau_chart_type: str) -> str:
        """Map Tableau chart type to Power BI visual type."""
        mapping = {
            'Bar Chart': 'Clustered Bar Chart',
            'Line Chart': 'Line Chart',
            'Scatter Plot': 'Scatter Chart',
            'Table': 'Table',
            'Map': 'Map',
            'Pie Chart': 'Pie Chart',
            'Heatmap': 'Matrix',
            'Treemap': 'Treemap'
        }
        return mapping.get(tableau_chart_type, 'Clustered Bar Chart')
    
    def _assess_visual_difficulty(self, worksheet: Dict) -> str:
        """Assess migration difficulty for a visual."""
        chart_type = worksheet['chart_type']
        field_count = len(worksheet['used_fields'])
        filter_count = len(worksheet['filters'])
        
        if chart_type in ['Table', 'Bar Chart'] and field_count < 3 and filter_count == 0:
            return "Easy"
        elif chart_type in ['Line Chart', 'Scatter Plot'] and field_count < 5:
            return "Medium"
        else:
            return "Hard"
    
    def _plan_calculated_field_migration(self, data: Dict) -> Dict[str, Any]:
        """Plan calculated field migration."""
        plan = {
            "total_calculations": len(data['calculated_fields']),
            "complexity_breakdown": {"Simple": 0, "Medium": 0, "Complex": 0},
            "migration_notes": [],
            "dax_conversions": []
        }
        
        for calc in data['calculated_fields']:
            complexity = calc['complexity']
            plan['complexity_breakdown'][complexity] += 1
            
            if calc['powerbi_equivalent']:
                plan['dax_conversions'].append({
                    "tableau_formula": calc['formula'],
                    "dax_equivalent": calc['powerbi_equivalent'],
                    "complexity": complexity
                })
        
        # Add migration notes
        if plan['complexity_breakdown']['Complex'] > 0:
            plan['migration_notes'].append("Some complex calculations may require manual review and testing")
        
        return plan
    
    def _plan_parameter_migration(self, data: Dict) -> Dict[str, Any]:
        """Plan parameter migration."""
        plan = {
            "total_parameters": len(data['parameters']),
            "parameter_types": {},
            "migration_approach": "Use Power BI parameters or slicers",
            "notes": []
        }
        
        for param in data['parameters']:
            param_type = param['type']
            if param_type not in plan['parameter_types']:
                plan['parameter_types'][param_type] = 0
            plan['parameter_types'][param_type] += 1
        
        return plan
    
    def _generate_migration_recommendations(self, data: Dict) -> List[str]:
        """Generate migration recommendations."""
        recommendations = []
        
        complexity = data['workbook_metadata']['complexity_score']
        
        if complexity == "High":
            recommendations.append("Consider breaking this workbook into multiple Power BI reports")
            recommendations.append("Plan for extended testing and validation phase")
        
        if len(data['calculated_fields']) > 10:
            recommendations.append("Review calculated fields for optimization opportunities")
        
        if data['workbook_metadata']['total_worksheets'] > 20:
            recommendations.append("Consider organizing visuals into multiple report pages")
        
        recommendations.extend([
            "Set up proper data refresh schedules in Power BI",
            "Configure row-level security if needed",
            "Plan for user training on Power BI interface",
            "Consider using Power BI Premium for better performance"
        ])
        
        return recommendations
