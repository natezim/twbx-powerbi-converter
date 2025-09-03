"""
Tableau Thumbnail Extractor

This module extracts thumbnail screenshots from Tableau TWB/TWBX files and saves them as PNG files.
Thumbnails are stored as Base64-encoded PNG data in the XML <thumbnails> section.
"""

import os
import re
import base64
from typing import Dict, List, Tuple, Optional
import xml.etree.ElementTree as ET


class ThumbnailExtractor:
    """Extracts and saves thumbnail images from Tableau workbooks."""
    
    def __init__(self):
        self.extracted_count = 0
        self.errors = []
    
    def extract_thumbnails(self, xml_root: ET.Element, output_dir: str) -> Dict[str, any]:
        """
        Extract all thumbnails from Tableau XML and save as PNG files.
        
        Args:
            xml_root: The root XML element of the Tableau workbook
            output_dir: Base output directory where screenshots folder will be created
            
        Returns:
            Dictionary with extraction results and metadata
        """
        print(f"🖼️  Extracting thumbnails from Tableau workbook...")
        
        # Initialize results
        results = {
            'extracted_count': 0,
            'saved_files': [],
            'errors': [],
            'screenshots_dir': ''
        }
        
        try:
            # Create screenshots directory
            screenshots_dir = self._create_screenshots_directory(output_dir)
            results['screenshots_dir'] = screenshots_dir
            
            # Find thumbnails section in XML
            thumbnails_section = xml_root.find('.//thumbnails')
            if thumbnails_section is None:
                print("   No thumbnails section found in workbook")
                return results
            
            # Extract all thumbnail elements
            thumbnail_elements = thumbnails_section.findall('thumbnail')
            if not thumbnail_elements:
                print("   No thumbnail elements found")
                return results
            
            print(f"   Found {len(thumbnail_elements)} thumbnail(s) to extract")
            
            # Process each thumbnail
            for i, thumbnail_elem in enumerate(thumbnail_elements):
                try:
                    file_info = self._extract_single_thumbnail(
                        thumbnail_elem, screenshots_dir, i + 1
                    )
                    if file_info:
                        results['saved_files'].append(file_info)
                        results['extracted_count'] += 1
                        print(f"   ✅ Saved: {file_info['filename']}")
                    
                except Exception as e:
                    error_msg = f"Failed to extract thumbnail {i + 1}: {str(e)}"
                    results['errors'].append(error_msg)
                    print(f"   ❌ {error_msg}")
            
            print(f"   📊 Successfully extracted {results['extracted_count']} thumbnail(s)")
            
        except Exception as e:
            error_msg = f"Thumbnail extraction failed: {str(e)}"
            results['errors'].append(error_msg)
            print(f"   ❌ {error_msg}")
        
        return results
    
    def _create_screenshots_directory(self, output_dir: str) -> str:
        """Create screenshots directory in the output folder."""
        screenshots_dir = os.path.join(output_dir, 'screenshots')
        os.makedirs(screenshots_dir, exist_ok=True)
        return screenshots_dir
    
    def _extract_single_thumbnail(self, thumbnail_elem: ET.Element, 
                                  screenshots_dir: str, thumbnail_number: int) -> Optional[Dict[str, any]]:
        """
        Extract a single thumbnail element and save as PNG file.
        
        Args:
            thumbnail_elem: The thumbnail XML element
            screenshots_dir: Directory to save the PNG file
            thumbnail_number: Sequential number for this thumbnail
            
        Returns:
            Dictionary with file information if successful, None if failed
        """
        # Get thumbnail attributes
        name = thumbnail_elem.get('name', f'Thumbnail_{thumbnail_number}')
        height = thumbnail_elem.get('height', 'Unknown')
        width = thumbnail_elem.get('width', 'Unknown')
        
        # Get Base64 data from element text
        base64_data = thumbnail_elem.text
        if not base64_data or not base64_data.strip():
            raise ValueError(f"No Base64 data found in thumbnail '{name}'")
        
        # Clean and validate Base64 data
        base64_data = base64_data.strip()
        
        try:
            # Decode Base64 to binary PNG data
            png_data = base64.b64decode(base64_data)
            
            # Validate PNG header (should start with PNG signature)
            if not png_data.startswith(b'\x89PNG\r\n\x1a\n'):
                raise ValueError("Decoded data is not a valid PNG file")
            
        except Exception as e:
            raise ValueError(f"Failed to decode Base64 data: {str(e)}")
        
        # Create safe filename
        safe_filename = self._create_safe_filename(name, screenshots_dir, thumbnail_number)
        file_path = os.path.join(screenshots_dir, safe_filename)
        
        # Save PNG file
        try:
            with open(file_path, 'wb') as f:
                f.write(png_data)
        except Exception as e:
            raise IOError(f"Failed to save PNG file '{safe_filename}': {str(e)}")
        
        # Return file information
        return {
            'filename': safe_filename,
            'file_path': file_path,
            'original_name': name,
            'dimensions': f"{width}x{height}",
            'file_size': len(png_data),
            'thumbnail_number': thumbnail_number
        }
    
    def _create_safe_filename(self, original_name: str, screenshots_dir: str, 
                              thumbnail_number: int) -> str:
        """
        Create a filesystem-safe filename, handling duplicates.
        
        Args:
            original_name: Original thumbnail name from XML
            screenshots_dir: Directory where file will be saved
            thumbnail_number: Sequential number for this thumbnail
            
        Returns:
            Safe filename with .png extension
        """
        # Sanitize the name for filesystem compatibility
        safe_name = self._sanitize_filename(original_name)
        
        # Ensure we have a base name
        if not safe_name or safe_name.isspace():
            safe_name = f"Thumbnail_{thumbnail_number}"
        
        # Add .png extension
        base_filename = f"{safe_name}.png"
        
        # Handle duplicates by appending numbers
        filename = base_filename
        counter = 1
        
        while os.path.exists(os.path.join(screenshots_dir, filename)):
            name_part = safe_name
            filename = f"{name_part}_{counter}.png"
            counter += 1
        
        return filename
    
    def _sanitize_filename(self, filename: str) -> str:
        """
        Sanitize filename for filesystem compatibility.
        
        Args:
            filename: Original filename
            
        Returns:
            Sanitized filename safe for filesystem
        """
        if not filename:
            return ""
        
        # Replace invalid characters with underscores
        # Invalid characters: < > : " | ? * \ /
        invalid_chars = r'[<>:"|?*\\/]'
        safe_name = re.sub(invalid_chars, '_', filename)
        
        # Replace multiple consecutive underscores with single underscore
        safe_name = re.sub(r'_+', '_', safe_name)
        
        # Remove leading/trailing whitespace and underscores
        safe_name = safe_name.strip().strip('_')
        
        # Limit length to avoid filesystem issues
        if len(safe_name) > 200:
            safe_name = safe_name[:200]
        
        # Ensure it's not empty after cleaning
        if not safe_name or safe_name.isspace():
            return ""
        
        return safe_name
    
    def get_extraction_summary(self, results: Dict[str, any]) -> str:
        """
        Generate a human-readable summary of the thumbnail extraction.
        
        Args:
            results: Results dictionary from extract_thumbnails()
            
        Returns:
            Formatted summary string
        """
        if not results:
            return "No thumbnail extraction results available."
        
        summary_lines = []
        summary_lines.append("🖼️  THUMBNAIL EXTRACTION SUMMARY")
        summary_lines.append("=" * 50)
        
        if results['extracted_count'] > 0:
            summary_lines.append(f"✅ Successfully extracted: {results['extracted_count']} thumbnail(s)")
            summary_lines.append(f"📁 Saved to: {results['screenshots_dir']}")
            summary_lines.append("")
            
            # List extracted files
            summary_lines.append("📋 Extracted Files:")
            for file_info in results['saved_files']:
                size_kb = file_info['file_size'] / 1024
                summary_lines.append(
                    f"   • {file_info['filename']} "
                    f"({file_info['dimensions']}, {size_kb:.1f} KB)"
                )
        else:
            summary_lines.append("ℹ️  No thumbnails were extracted")
        
        # Add errors if any
        if results['errors']:
            summary_lines.append("")
            summary_lines.append("⚠️  Errors encountered:")
            for error in results['errors']:
                summary_lines.append(f"   • {error}")
        
        return "\n".join(summary_lines)
