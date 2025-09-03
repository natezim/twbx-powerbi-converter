#!/usr/bin/env python3
"""
Improved GUI for Tableau to Power BI Converter
Features:
- Separate buttons for Analysis and Data Extraction
- Datasource dropdown with basic info
- Progress tracking for large files
- Better error handling
- Automatic directory opening after export
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import os
import threading
from pathlib import Path
import sys
import subprocess
import platform

# Add the core directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'core'))

from main import TableauMigrator
from core.field_extractor import FieldExtractor
from core.enhanced_migrator import EnhancedTableauMigrator


class ImprovedTableauConverterGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Tableau to Power BI Converter - Improved Edition")
        self.root.geometry("800x700")
        
        # Initialize variables
        self.selected_file = None
        self.migrator = None
        self.enhanced_migrator = None
        self.data_sources = []
        self.current_analysis = None

        
        # Create GUI elements
        self.create_widgets()
        
    def create_widgets(self):
        """Create all GUI widgets."""
        # Main frame
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        
        # File selection section
        ttk.Label(main_frame, text="Tableau File:", font=('Arial', 12, 'bold')).grid(row=0, column=0, sticky=tk.W, pady=(0, 5))
        
        file_frame = ttk.Frame(main_frame)
        file_frame.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        file_frame.columnconfigure(0, weight=1)
        
        self.file_label = ttk.Label(file_frame, text="No file selected", foreground="gray")
        self.file_label.grid(row=0, column=0, sticky=(tk.W, tk.E), padx=(0, 10))
        
        ttk.Button(file_frame, text="Browse", command=self.browse_file).grid(row=0, column=1)
        
        # Removed datasource dropdown and info section for streamlined version
        
        # Action buttons section
        button_frame = ttk.Frame(main_frame)
        button_frame.grid(row=2, column=0, columnspan=2, pady=20)
        
        self.analyze_btn = ttk.Button(button_frame, text="🚀 Analyze & Extract All Data", command=self.analyze_twbx, style="Accent.TButton")
        self.analyze_btn.pack(side=tk.LEFT, padx=(0, 10))
        
        self.extract_btn = ttk.Button(button_frame, text="📊 Extract Hyper Data", command=self.extract_hyper_data, state="disabled")
        self.extract_btn.pack(side=tk.LEFT)
        
        # Progress section
        ttk.Label(main_frame, text="Progress:", font=('Arial', 12, 'bold')).grid(row=3, column=0, sticky=tk.W, pady=(20, 5))
        
        self.progress_var = tk.StringVar(value="Ready")
        self.progress_label = ttk.Label(main_frame, textvariable=self.progress_var, foreground="blue")
        self.progress_label.grid(row=4, column=0, columnspan=2, sticky=tk.W, pady=(0, 5))
        
        self.progress_bar = ttk.Progressbar(main_frame, mode='indeterminate')
        self.progress_bar.grid(row=5, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        
        # Log section
        ttk.Label(main_frame, text="Log:", font=('Arial', 12, 'bold')).grid(row=6, column=0, sticky=tk.W, pady=(10, 5))
        
        self.log_text = scrolledtext.ScrolledText(main_frame, height=12, width=80)
        self.log_text.grid(row=7, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        
        # Configure row weights for proper resizing
        main_frame.rowconfigure(7, weight=1)
        
        # Status bar
        self.status_var = tk.StringVar(value="Ready")
        status_bar = ttk.Label(main_frame, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W)
        status_bar.grid(row=8, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(10, 0))
        
    def browse_file(self):
        """Browse for TWBX or TWB file."""
        file_path = filedialog.askopenfilename(
            title="Select Tableau File",
            filetypes=[("Tableau files", "*.twbx;*.twb"), ("TWBX files", "*.twbx"), ("TWB files", "*.twb"), ("All files", "*.*")]
        )
        
        if file_path:
            self.selected_file = file_path
            self.file_label.config(text=os.path.basename(file_path), foreground="black")
            self.status_var.set(f"Selected: {os.path.basename(file_path)}")
            
            # Reset state
            self.data_sources = []
            self.current_analysis = None
            self.extract_btn.config(state="disabled")
            
            # Enable analyze button
            self.analyze_btn.config(state="normal")

            
    def analyze_twbx(self):
        """Analyze the TWBX file to extract metadata."""
        if not self.selected_file:
            messagebox.showerror("Error", "Please select a TWBX file first.")
            return
        
        # Disable buttons during analysis
        self.analyze_btn.config(state="disabled")
        self.extract_btn.config(state="disabled")
        
        # Start progress
        self.progress_var.set("Analyzing and extracting all data...")
        self.progress_bar.start()
        
        # Clear previous results
        self.log_text.delete(1.0, tk.END)
        
        # Run analysis in separate thread
        thread = threading.Thread(target=self._analyze_twbx_thread)
        thread.daemon = True
        thread.start()
        
    def _analyze_twbx_thread(self):
        """Run comprehensive analysis and extraction in background thread."""
        try:
            # Initialize enhanced migrator for comprehensive extraction
            self.enhanced_migrator = EnhancedTableauMigrator()
            
            # Perform comprehensive extraction (streamlined - single JSON file only)
            comprehensive_data = self.enhanced_migrator.process_tableau_file_comprehensive(
                self.selected_file, 
                output_format="json"
            )
            
            if comprehensive_data:
                # Update GUI on main thread with comprehensive data
                self.root.after(0, self._analysis_complete, comprehensive_data)
            else:
                self.root.after(0, self._analysis_failed, "Failed to extract comprehensive data")
                
        except Exception as e:
            self.root.after(0, self._analysis_failed, str(e))
    
    def _analysis_complete(self, comprehensive_data):
        """Handle successful comprehensive analysis and extraction completion."""
        self.progress_bar.stop()
        self.progress_var.set("Analysis and extraction complete!")
        
        # Store the comprehensive data
        self.current_analysis = comprehensive_data
        
        # No need for datasource dropdown in streamlined version
        
        # Enable buttons
        self.analyze_btn.config(state="normal")
        # Always enable extract button after analysis (it will check for Hyper data when clicked)
        self.extract_btn.config(state="normal")
        
        # Update status
        self.status_var.set("Analysis and extraction complete - Ready for Hyper data extraction")
        
        # Log success with detailed summary
        self.log_text.insert(tk.END, f"✅ Analysis and extraction complete!\n")
        self.log_text.insert(tk.END, f"{'='*60}\n")
        
        # Get summary info
        workbook_info = comprehensive_data.get("workbook_info", {})
        workbook_metadata = comprehensive_data.get("workbook_metadata", {})
        worksheets = comprehensive_data.get("worksheets", [])
        dashboards = comprehensive_data.get("dashboards", [])
        fields_comp = comprehensive_data.get("fields_comprehensive", {})
        datasources = comprehensive_data.get("datasources", [])
        
        # Basic summary
        self.log_text.insert(tk.END, f"📊 WORKBOOK SUMMARY:\n")
        self.log_text.insert(tk.END, f"   Name: {workbook_info.get('name', workbook_metadata.get('name', 'Unknown'))}\n")
        self.log_text.insert(tk.END, f"   Author: {workbook_metadata.get('author', 'Unknown')}\n")
        self.log_text.insert(tk.END, f"   Version: {workbook_metadata.get('version', 'Unknown')}\n")
        self.log_text.insert(tk.END, f"   Modified: {workbook_metadata.get('modified_date', 'Unknown')}\n")
        self.log_text.insert(tk.END, f"   Complexity: {workbook_info.get('complexity_score', workbook_metadata.get('complexity_score', 'Unknown'))}\n")
        self.log_text.insert(tk.END, f"\n📈 CONTENT OVERVIEW:\n")
        self.log_text.insert(tk.END, f"   Worksheets: {len(worksheets)}\n")
        self.log_text.insert(tk.END, f"   Dashboards: {len(dashboards)}\n")
        self.log_text.insert(tk.END, f"   Data Sources: {len(datasources)}\n")
        self.log_text.insert(tk.END, f"   Total Fields: {fields_comp.get('total_fields', 0)}\n")
        self.log_text.insert(tk.END, f"   Calculated Fields: {len(fields_comp.get('calculated_fields', []))}\n")
        self.log_text.insert(tk.END, f"   Parameters: {len(fields_comp.get('parameters', []))}\n")
        self.log_text.insert(tk.END, f"\n📄 OUTPUT FILES:\n")
        self.log_text.insert(tk.END, f"   • Comprehensive JSON: All data in one file\n")
        self.log_text.insert(tk.END, f"   • Thumbnail screenshots: Visual previews\n")
        
        # Show detailed connection info
        self.log_text.insert(tk.END, f"\n🔌 DATA SOURCE CONNECTIONS:\n")
        for i, ds in enumerate(datasources, 1):
            ds_name = ds.get('caption', 'Unknown')
            ds_id = ds.get('name', 'Unknown')
            field_count = ds.get('field_count', 0)
            
            self.log_text.insert(tk.END, f"   {i}. {ds_name}\n")
            self.log_text.insert(tk.END, f"      ID: {ds_id}\n")
            self.log_text.insert(tk.END, f"      Fields: {field_count}\n")
            
            # Show connections
            connections = ds.get('connections', [])
            if connections:
                for conn in connections:
                    conn_type = conn.get('class', 'Unknown')
                    username = conn.get('username', 'Unknown')
                    
                    # Try different connection field formats
                    catalog = conn.get('CATALOG', conn.get('catalog', ''))
                    exec_catalog = conn.get('EXECCATALOG', conn.get('exec_catalog', ''))
                    project = conn.get('project', '')
                    server = conn.get('server', '')
                    dbname = conn.get('dbname', '')
                    
                    # Build connection display
                    if catalog and exec_catalog:
                        self.log_text.insert(tk.END, f"      Connection: {conn_type} → {catalog} (Exec: {exec_catalog})\n")
                    elif project:
                        self.log_text.insert(tk.END, f"      Connection: {conn_type} → Project: {project}\n")
                    elif server and dbname:
                        self.log_text.insert(tk.END, f"      Connection: {conn_type} → {server}/{dbname}\n")
                    else:
                        self.log_text.insert(tk.END, f"      Connection: {conn_type}\n")
                    
                    if username and username != 'Unknown':
                        self.log_text.insert(tk.END, f"      User: {username}\n")
            else:
                self.log_text.insert(tk.END, f"      Connection: No connection details\n")
            self.log_text.insert(tk.END, f"\n")
        
        # Show field breakdown
        self.log_text.insert(tk.END, f"\n📊 FIELD BREAKDOWN:\n")
        regular_fields = fields_comp.get('regular_fields', [])
        calculated_fields = fields_comp.get('calculated_fields', [])
        parameters = fields_comp.get('parameters', [])
        
        self.log_text.insert(tk.END, f"   • Regular Fields: {len(regular_fields)}\n")
        self.log_text.insert(tk.END, f"   • Calculated Fields: {len(calculated_fields)}\n")
        self.log_text.insert(tk.END, f"   • Parameters: {len(parameters)}\n")
        
        # Show sample fields from each datasource using field_metadata
        self.log_text.insert(tk.END, f"\n📋 SAMPLE FIELDS BY DATASOURCE:\n")
        field_metadata = comprehensive_data.get("workbook_metadata", {}).get("field_metadata", {})
        all_fields = field_metadata.get("all_fields", [])
        
        # Group fields by datasource
        fields_by_datasource = {}
        for field in all_fields:
            ds_name = field.get('datasource', 'Unknown')
            if ds_name not in fields_by_datasource:
                fields_by_datasource[ds_name] = []
            fields_by_datasource[ds_name].append(field)
        
        # Show sample fields for each datasource
        for ds in datasources:
            ds_name = ds.get('caption', 'Unknown')
            ds_id = ds.get('name', 'Unknown')
            
            # Find fields for this datasource
            ds_fields = fields_by_datasource.get(ds_id, [])
            if ds_fields:
                sample_fields = ds_fields[:5]  # Show first 5 fields
                field_names = [f.get('caption', f.get('name', 'Unknown')) for f in sample_fields]
                self.log_text.insert(tk.END, f"   • {ds_name}: {', '.join(field_names)}\n")
                if len(ds_fields) > 5:
                    self.log_text.insert(tk.END, f"     ... and {len(ds_fields) - 5} more fields\n")
            else:
                self.log_text.insert(tk.END, f"   • {ds_name}: No fields found\n")
        
        # Show worksheet details
        self.log_text.insert(tk.END, f"\n📊 WORKSHEET DETAILS:\n")
        for ws in worksheets:
            ws_name = ws.get('name', 'Unknown')
            chart_type = ws.get('chart_type', 'Unknown')
            fields_used = ws.get('fields_used', [])
            filters = ws.get('filters', [])
            
            self.log_text.insert(tk.END, f"   • {ws_name} ({chart_type})\n")
            self.log_text.insert(tk.END, f"     - Fields used: {len(fields_used)}\n")
            self.log_text.insert(tk.END, f"     - Filters: {len(filters)}\n")
            
            # Show sample fields used
            if fields_used:
                sample_field_names = [f.get('caption', f.get('name', 'Unknown')) for f in fields_used[:3]]
                self.log_text.insert(tk.END, f"     - Sample fields: {', '.join(sample_field_names)}\n")
                if len(fields_used) > 3:
                    self.log_text.insert(tk.END, f"       ... and {len(fields_used) - 3} more\n")
            
            # Show filter details if any
            if filters:
                filter_names = [f.get('caption', f.get('name', 'Unknown')) for f in filters[:2]]
                self.log_text.insert(tk.END, f"     - Sample filters: {', '.join(filter_names)}\n")
                if len(filters) > 2:
                    self.log_text.insert(tk.END, f"       ... and {len(filters) - 2} more\n")
        self.log_text.see(tk.END)
        
        # Show success message
        messagebox.showinfo("Analysis and Extraction Complete", 
                          f"Successfully analyzed and extracted all data!\n\n"
                          f"Workbook: {workbook_metadata.get('name', 'Unknown')}\n"
                          f"Worksheets: {len(worksheets)}\n"
                          f"Dashboards: {len(dashboards)}\n"
                          f"Data Sources: {len(datasources)}\n"
                          f"Total Fields: {fields_comp.get('total_fields', 0)}\n\n"
                          f"All data exported to a single comprehensive JSON file.")
    
    def _analysis_failed(self, error_msg):
        """Handle analysis failure."""
        self.progress_bar.stop()
        self.progress_var.set("Analysis failed!")
        
        # Re-enable analyze button
        self.analyze_btn.config(state="normal")

        
        # Show error
        messagebox.showerror("Analysis Error", f"Failed to analyze TWBX file:\n{error_msg}")
        self.status_var.set("Analysis failed")
        
        # Log error
        self.log_text.insert(tk.END, f"❌ Analysis failed: {error_msg}\n")
        self.log_text.see(tk.END)
    
    # Removed datasource selection methods for streamlined version
    
    # Removed _display_datasource_info method for streamlined version
    
    def extract_hyper_data(self):
        """Extract Hyper data from the analyzed datasources."""
        if not self.current_analysis:
            messagebox.showerror("Error", "Please analyze a TWBX file first.")
            return
        
        # For now, just show a message that Hyper extraction is not implemented in streamlined version
        messagebox.showinfo("Hyper Data Extraction", 
            "Hyper data extraction is not available in the streamlined version.\n\n"
            "All data has been extracted to the comprehensive JSON file.")
                return
        
        # Disable buttons during extraction
        self.extract_btn.config(state="disabled")

        
        # Start progress
        self.progress_var.set("Extracting data...")
        self.progress_bar.start()
        
        # Run extraction in separate thread
        thread = threading.Thread(target=self._extract_data_thread)
        thread.daemon = True
        thread.start()
    
    def _extract_data_thread(self):
        """Run data extraction in background thread."""
        try:
            # First, we need to extract Hyper data from the TWBX file
            # Process the file again but this time WITH Hyper extraction
            self.data_sources = self.migrator.process_tableau_file(self.selected_file, skip_hyper_extraction=False)
            
            if not self.data_sources:
                self.root.after(0, self._extraction_failed, "Failed to process TWBX file for Hyper data extraction")
                return
            
            # Find the selected datasource
            selected_name = self.datasource_var.get()
            selected_ds = None
            for ds in self.data_sources:
                name = ds.get('caption') or ds.get('name') or 'Unknown'
                if name == selected_name:
                    selected_ds = ds
                    break
            
            if selected_ds and selected_ds.get('hyper_data'):
                # Check if Hyper API dependency is missing
                if selected_ds['hyper_data'].get("__missing_dependency__"):
                    missing_dep = selected_ds['hyper_data']['__missing_dependency__']
                    self.root.after(0, self._extraction_failed, 
                        f"Hyper data extraction requires {missing_dep}. Install with: pip install {missing_dep}")
                    return
                
                # Export hyper data to Excel
                try:
                    # Create workbook-specific folder for hyper data
                    workbook_name = selected_ds.get('workbook_name', 'Unknown')
                    if workbook_name is None:
                        workbook_name = 'Unknown'
                    safe_workbook = str(workbook_name).replace(' ', '_').replace('/', '_')
                    safe_workbook = ''.join(c for c in safe_workbook if c.isalnum() or c in '_-')
                    hyper_output_dir = os.path.join('output', safe_workbook)
                    os.makedirs(hyper_output_dir, exist_ok=True)
                    
                    # Export hyper data to Excel
                    exported_files = self.migrator.field_extractor.export_hyper_data_to_excel(
                        selected_ds['hyper_data'], hyper_output_dir
                    )
                    
                    if exported_files:
                        self.root.after(0, self._extraction_complete, f"Saved {len(exported_files)} Excel files")
                    else:
                        self.root.after(0, self._extraction_complete, "Hyper data extracted but no files were saved")
                except Exception as e:
                    self.root.after(0, self._extraction_failed, f"Error saving hyper data: {str(e)}")
            else:
                # No hyper data available
                self.root.after(0, self._extraction_complete, "No Hyper data found in this datasource")
                
        except Exception as e:
            self.root.after(0, self._extraction_failed, str(e))
    
    def _extraction_complete(self, message=None):
        """Handle successful data extraction completion."""
        self.progress_bar.stop()
        if message:
            self.progress_var.set(f"Extraction complete: {message}")
        else:
            self.progress_var.set("Data extraction complete!")
        
        # Enable buttons
        self.extract_btn.config(state="normal")
        
        # Update status
        self.status_var.set("Data extraction complete")
        
        # Log success
        if message:
            self.log_text.insert(tk.END, f"ℹ️ {message}\n")
        else:
            self.log_text.insert(tk.END, f"✅ Data extraction complete!\n")
        self.log_text.see(tk.END)
    
    def _extraction_failed(self, error_msg):
        """Handle data extraction failure."""
        self.progress_bar.stop()
        self.progress_var.set("Extraction failed!")
        
        # Re-enable extract button
        self.extract_btn.config(state="normal")
        
        # Show error
        messagebox.showerror("Extraction Error", f"Failed to extract data:\n{error_msg}")
        self.status_var.set("Data extraction failed")
        
        # Log error
        self.log_text.insert(tk.END, f"❌ Data extraction failed: {error_msg}\n")
        self.log_text.see(tk.END)
    

    



def main():
    """Main entry point for the improved GUI."""
    root = tk.Tk()
    
    # Set theme if available
    try:
        style = ttk.Style()
        style.theme_use('clam')
    except:
        pass
    
    app = ImprovedTableauConverterGUI(root)
    
    # Center the window
    root.update_idletasks()
    x = (root.winfo_screenwidth() // 2) - (root.winfo_width() // 2)
    y = (root.winfo_screenheight() // 2) - (root.winfo_height() // 2)
    root.geometry(f"+{x}+{y}")
    
    root.mainloop()


if __name__ == "__main__":
    main()
