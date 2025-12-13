#!/usr/bin/env python
# -*- coding: utf-8 -*-

import subprocess
import sys
import os
import webbrowser
import time
from threading import Thread


def check_dependencies():
    """Check and install required packages"""
    required_packages = ['streamlit', 'plotly', 'pandas', 'numpy', 'pyodbc', 'openpyxl']

    print("Checking dependencies...")
    for package in required_packages:
        try:
            __import__(package)
            print(f"  ✅ {package}")
        except ImportError:
            print(f"  📦 Installing {package}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])


def open_browser():
    """Open browser after delay"""
    time.sleep(3)
    webbrowser.open("http://localhost:8501")


def run_streamlit_dashboard():
    """Run the Streamlit dashboard"""
    print("\nStarting Streamlit dashboard...")
    print("The dashboard will open in your browser automatically.")
    print("Press Ctrl+C to stop the server.")
    print("-" * 50)

    # Open browser in background
    Thread(target=open_browser).start()

    # Run Streamlit
    script_dir = os.path.dirname(os.path.abspath(__file__))
    dashboard_path = os.path.join(script_dir, "dashboard.py")

    subprocess.call([sys.executable, "-m", "streamlit", "run", dashboard_path])


def main():
    """Main function"""
    print("\n" + "=" * 50)
    print("     NORTHWIND STREAMLIT DASHBOARD")
    print("=" * 50)

    # Check dependencies
    check_dependencies()

    # Run the dashboard
    run_streamlit_dashboard()


if __name__ == "__main__":
    # Change to script directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    main()