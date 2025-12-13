# config.py
import pyodbc
import pandas as pd
from sqlalchemy import create_engine, text
import warnings

warnings.filterwarnings('ignore')





class DatabaseConfig:
    # settings
    SQL_SERVER_INSTANCE = 'DESKTOP-CIESELA\\SQLEXPRESS'

    SOURCE_DATABASE = 'Northwind'
    TARGET_DATABASE = 'Dw'

    ACCESS_DB_PATH = r'C:\Users\amery\Desktop\Northwind.accdb'


def connect_sql_server():
    try:
        conn = pyodbc.connect(
            'DRIVER={SQL Server};'
            f'SERVER={DatabaseConfig.SQL_SERVER_INSTANCE};'
            f'DATABASE={DatabaseConfig.SOURCE_DATABASE};'
            'Trusted_Connection=yes;'
        )
        print(f"✅ Connected to SQL Server ({DatabaseConfig.SOURCE_DATABASE})")
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to SQL Server: {e}")
        return None


def connect_data_werehouse():
    try:
        conn = pyodbc.connect(
            'DRIVER={SQL Server};'
            f'SERVER={DatabaseConfig.SQL_SERVER_INSTANCE};'
            f'DATABASE={DatabaseConfig.TARGET_DATABASE};'
            'Trusted_Connection=yes;'
        )
        print(f"✅ Connected to Data Warehouse ({DatabaseConfig.TARGET_DATABASE})")
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to Data Warehouse: {e}")
        return None



# Main execution for testing
if __name__ == "__main__":
