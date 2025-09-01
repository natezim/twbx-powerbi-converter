#!/usr/bin/env python3
"""
Tableau Parser Module
Handles TWBX extraction and XML parsing using local Tableau Document API
"""

import zipfile
import xml.etree.ElementTree as ET
from .tableaudocumentapi.workbook import Workbook
from .tableaudocumentapi.datasource import Datasource


class TableauParser:
    """Handles TWBX file extraction and XML parsing."""
    
    def __init__(self, twbx_path):
        self.twbx_path = twbx_path
        self.workbook = None
        self.xml_root = None
    
    def extract_and_parse(self):
        """Extract TWBX and parse using official Tableau API + XML."""
        try:
            # Use official Tableau API for workbook access
            self.workbook = Workbook(self.twbx_path)
            
            # Also extract XML for rich metadata
            with zipfile.ZipFile(self.twbx_path, 'r') as z:
                # Find the .twb file
                twb_files = [f for f in z.namelist() if f.endswith('.twb')]
                if twb_files:
                    twb_content = z.read(twb_files[0]).decode('utf-8')
                    self.xml_root = ET.fromstring(twb_content)
            
            return True
        except Exception as e:
            print(f"❌ Failed to parse: {e}")
            return False
    
    def get_workbook(self):
        """Get the parsed Tableau workbook."""
        return self.workbook
    
    def get_xml_root(self):
        """Get the parsed XML root element."""
        return self.xml_root
    
    def find_datasource_xml(self, datasource_name):
        """Find the XML element for a specific datasource."""
        if not self.xml_root:
            return None
        
        for datasource in self.xml_root.findall('.//datasource'):
            if datasource.get('name') == datasource_name:
                return datasource
        return None
