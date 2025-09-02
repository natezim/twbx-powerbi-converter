#!/usr/bin/env python3
"""
Tableau to Power BI Converter Launcher
Choose between GUI and CLI modes
"""

import sys
import os

def main():
    """Main launcher that asks user to choose between GUI and CLI."""
    print("🔍 Tableau to Power BI Converter")
    print("=" * 50)
    print()
    print("Choose your preferred mode:")
    print("1. GUI Mode (Recommended for most users)")
    print("2. Command Line Mode (For automation/scripting)")
    print("3. Exit")
    print()
    
    while True:
        try:
            choice = input("Enter your choice (1-3): ").strip()
            
            if choice == "1":
                print("\n🚀 Starting GUI mode...")
                try:
                    from gui_interface import main as gui_main
                    gui_main()
                except ImportError as e:
                    print(f"❌ Error starting GUI: {e}")
                    print("Make sure tkinter is available on your system.")
                    input("Press Enter to continue...")
                break
                
            elif choice == "2":
                print("\n💻 Starting command line mode...")
                try:
                    from main import main as cli_main
                    cli_main()
                except ImportError as e:
                    print(f"❌ Error starting CLI: {e}")
                    input("Press Enter to continue...")
                break
                
            elif choice == "3":
                print("\n👋 Goodbye!")
                break
                
            else:
                print("❌ Invalid choice. Please enter 1, 2, or 3.")
                
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            input("Press Enter to continue...")

if __name__ == "__main__":
    main()
