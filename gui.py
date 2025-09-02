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
from field_extractor import FieldExtractor


class ImprovedTableauConverterGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Tableau to Power BI Converter - Improved Edition")
        self.root.geometry("800x700")
        
        # Initialize variables
        self.selected_file = None
        self.migrator = None
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
        ttk.Label(main_frame, text="TWBX File:", font=('Arial', 12, 'bold')).grid(row=0, column=0, sticky=tk.W, pady=(0, 5))
        
        file_frame = ttk.Frame(main_frame)
        file_frame.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        file_frame.columnconfigure(0, weight=1)
        
        self.file_label = ttk.Label(file_frame, text="No file selected", foreground="gray")
        self.file_label.grid(row=0, column=0, sticky=(tk.W, tk.E), padx=(0, 10))
        
        ttk.Button(file_frame, text="Browse", command=self.browse_file).grid(row=0, column=1)
        
        # Datasource selection section
        ttk.Label(main_frame, text="Datasource:", font=('Arial', 12, 'bold')).grid(row=2, column=0, sticky=tk.W, pady=(10, 5))
        
        self.datasource_var = tk.StringVar()
        self.datasource_combo = ttk.Combobox(main_frame, textvariable=self.datasource_var, state="readonly", width=50)
        self.datasource_combo.grid(row=3, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        self.datasource_combo.bind('<<ComboboxSelected>>', self.on_datasource_selected)
        
        # Datasource info section
        ttk.Label(main_frame, text="Datasource Info:", font=('Arial', 12, 'bold')).grid(row=4, column=0, sticky=tk.W, pady=(10, 5))
        
        self.info_text = scrolledtext.ScrolledText(main_frame, height=8, width=80)
        self.info_text.grid(row=5, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        
        # Action buttons section
        button_frame = ttk.Frame(main_frame)
        button_frame.grid(row=6, column=0, columnspan=2, pady=20)
        
        self.analyze_btn = ttk.Button(button_frame, text="🔍 Analyze TWBX", command=self.analyze_twbx, style="Accent.TButton")
        self.analyze_btn.pack(side=tk.LEFT, padx=(0, 10))
        
        self.extract_btn = ttk.Button(button_frame, text="📊 Extract Hyper Data", command=self.extract_hyper_data, state="disabled")
        self.extract_btn.pack(side=tk.LEFT)
        
        # Progress section
        ttk.Label(main_frame, text="Progress:", font=('Arial', 12, 'bold')).grid(row=7, column=0, sticky=tk.W, pady=(20, 5))
        
        self.progress_var = tk.StringVar(value="Ready")
        self.progress_label = ttk.Label(main_frame, textvariable=self.progress_var, foreground="blue")
        self.progress_label.grid(row=8, column=0, columnspan=2, sticky=tk.W, pady=(0, 5))
        
        self.progress_bar = ttk.Progressbar(main_frame, mode='indeterminate')
        self.progress_bar.grid(row=9, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        
        # Log section
        ttk.Label(main_frame, text="Log:", font=('Arial', 12, 'bold')).grid(row=10, column=0, sticky=tk.W, pady=(10, 5))
        
        self.log_text = scrolledtext.ScrolledText(main_frame, height=12, width=80)
        self.log_text.grid(row=11, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        
        # Configure row weights for proper resizing
        main_frame.rowconfigure(11, weight=1)
        
        # Status bar
        self.status_var = tk.StringVar(value="Ready")
        status_bar = ttk.Label(main_frame, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W)
        status_bar.grid(row=12, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(10, 0))
        
    def browse_file(self):
        """Browse for TWBX file."""
        file_path = filedialog.askopenfilename(
            title="Select TWBX File",
            filetypes=[("Tableau files", "*.twbx"), ("All files", "*.*")]
        )
        
        if file_path:
            self.selected_file = file_path
            self.file_label.config(text=os.path.basename(file_path), foreground="black")
            self.status_var.set(f"Selected: {os.path.basename(file_path)}")
            
            # Reset state
            self.data_sources = []
            self.current_analysis = None
            self.datasource_combo.set('')
            self.datasource_combo['values'] = []
            self.info_text.delete(1.0, tk.END)
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
        self.progress_var.set("Analyzing TWBX file...")
        self.progress_bar.start()
        
        # Clear previous results
        self.info_text.delete(1.0, tk.END)
        self.log_text.delete(1.0, tk.END)
        
        # Run analysis in separate thread
        thread = threading.Thread(target=self._analyze_twbx_thread)
        thread.daemon = True
        thread.start()
        
    def _analyze_twbx_thread(self):
        """Run TWBX analysis in background thread."""
        try:
            # Initialize migrator
            self.migrator = TableauMigrator()
            
            # Process the file (skip Hyper extraction for analysis mode)
            self.data_sources = self.migrator.process_twbx_file(self.selected_file, skip_hyper_extraction=True)
            
            if self.data_sources:
                # Export regular results (CSV, setup guides) but skip Hyper data
                try:
                    self.migrator.export_results(self.data_sources, skip_hyper_data=True)
                    # Update GUI on main thread
                    self.root.after(0, self._analysis_complete)
                except Exception as e:
                    self.root.after(0, self._analysis_failed, f"Failed to export results: {str(e)}")
            else:
                self.root.after(0, self._analysis_failed, "Failed to process TWBX file")
                
        except Exception as e:
            self.root.after(0, self._analysis_failed, str(e))
    
    def _analysis_complete(self):
        """Handle successful analysis completion."""
        self.progress_bar.stop()
        self.progress_var.set("Analysis complete!")
        
        # Populate datasource dropdown
        datasource_names = []
        for ds in self.data_sources:
            name = ds.get('caption') or ds.get('name') or 'Unknown'
            datasource_names.append(name)
        
        self.datasource_combo['values'] = datasource_names
        if datasource_names:
            self.datasource_combo.set(datasource_names[0])
            self.on_datasource_selected()
        
        # Enable buttons
        self.analyze_btn.config(state="normal")
        # Always enable extract button after analysis (it will check for Hyper data when clicked)
        self.extract_btn.config(state="normal")
        
        # Update status
        self.status_var.set(f"Analysis complete: {len(self.data_sources)} datasource(s) found")
        
        # Log success
        self.log_text.insert(tk.END, f"✅ Analysis complete!\n")
        self.log_text.insert(tk.END, f"Found {len(self.data_sources)} datasource(s)\n")
        self.log_text.insert(tk.END, f"✅ Regular files exported (CSV, setup guides)\n")
        
        # Check if any datasources have missing Hyper API dependency
        missing_hyper_api = False
        for ds in self.data_sources:
            if ds.get('hyper_data') and ds['hyper_data'].get("__missing_dependency__"):
                missing_hyper_api = True
                break
        
        if missing_hyper_api:
            self.log_text.insert(tk.END, f"⚠️ Note: Some datasources have Hyper data but tableauhyperapi is not installed\n")
            self.log_text.insert(tk.END, f"   Install with: pip install tableauhyperapi to enable Hyper data extraction\n")
        
        self.log_text.see(tk.END)
    
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
    
    def on_datasource_selected(self, event=None):
        """Handle datasource selection."""
        selected_name = self.datasource_var.get()
        if not selected_name or not self.data_sources:
            return
        
        # Find the selected datasource
        selected_ds = None
        for ds in self.data_sources:
            name = ds.get('caption') or ds.get('name') or 'Unknown'
            if name == selected_name:
                selected_ds = ds
                break
        
        if selected_ds:
            self._display_datasource_info(selected_ds)
    
    def _display_datasource_info(self, datasource):
        """Display information about the selected datasource."""
        self.info_text.delete(1.0, tk.END)
        
        # Basic info
        info = f"📊 DATASOURCE INFORMATION\n"
        info += f"{'='*50}\n\n"
        info += f"Name: {datasource.get('name', 'N/A')}\n"
        info += f"Caption: {datasource.get('caption', 'N/A')}\n"
        info += f"Fields: {datasource.get('field_count', 0)}\n"
        
        # Connection info
        if datasource.get('connections'):
            info += f"\n🔌 CONNECTIONS:\n"
            for i, conn in enumerate(datasource['connections'], 1):
                info += f"  {i}. Type: {conn.get('dbclass', 'N/A')}\n"
                if conn.get('server'):
                    info += f"     Server: {conn.get('server')}\n"
                if conn.get('dbname'):
                    info += f"     Database: {conn.get('dbname')}\n"
                if conn.get('username'):
                    info += f"     Username: {conn.get('username')}\n"
                info += "\n"
        
        # Field summary
        fields = datasource.get('fields', [])
        if fields:
            used_fields = sum(1 for f in fields if f.get('used_in_workbook', False))
            calc_fields = sum(1 for f in fields if f.get('is_calculated', False))
            param_fields = sum(1 for f in fields if f.get('is_parameter', False))
            
            info += f"📋 FIELDS SUMMARY:\n"
            info += f"  Total: {len(fields)}\n"
            info += f"  Used in workbook: {used_fields}\n"
            info += f"  Calculated: {calc_fields}\n"
            info += f"  Parameters: {param_fields}\n"
        
        # Hyper data info
        if datasource.get('hyper_data'):
            # Check if Hyper API dependency is missing
            if datasource['hyper_data'].get("__missing_dependency__"):
                info += f"\n⚠️ HYPER DATA EXTRACTION:\n"
                info += f"  Status: Skipped - {datasource['hyper_data']['__missing_dependency__']} not available\n"
                info += f"  Install with: pip install tableauhyperapi\n"
            else:
                hyper_tables = len(datasource['hyper_data'])
                total_rows = sum(table_info['row_count'] for table_info in datasource['hyper_data'].values())
                info += f"\n💾 HYPER DATA:\n"
                info += f"  Tables: {hyper_tables}\n"
                info += f"  Total rows: {total_rows:,}\n"
        
        self.info_text.insert(1.0, info)
    
    def extract_hyper_data(self):
        """Extract Hyper data from the selected datasource."""
        if not self.data_sources or not self.datasource_var.get():
            messagebox.showerror("Error", "Please analyze a TWBX file first and select a datasource.")
            return
        
        # Check if the selected datasource has Hyper data and if it's available
        selected_name = self.datasource_var.get()
        selected_ds = None
        for ds in self.data_sources:
            name = ds.get('caption') or ds.get('name') or 'Unknown'
            if name == selected_name:
                selected_ds = ds
                break
        
        if selected_ds and selected_ds.get('hyper_data'):
            if selected_ds['hyper_data'].get("__missing_dependency__"):
                missing_dep = selected_ds['hyper_data']['__missing_dependency__']
                messagebox.showwarning("Missing Dependency", 
                    f"Hyper data extraction requires {missing_dep}.\n\n"
                    f"Install with: pip install {missing_dep}\n\n"
                    "After installation, restart the application and try again.")
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
            self.data_sources = self.migrator.process_twbx_file(self.selected_file, skip_hyper_extraction=False)
            
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
