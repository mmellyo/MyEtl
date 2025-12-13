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
    ACCESS_DB_PATH = r'C:\Users\amery\Desktop\Nw.accdb'


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


def connect_data_warehouse():
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
    # Test connections
    print("Testing SQL Server connection...")
    sql_conn = connect_sql_server()
    if sql_conn:
        sql_conn.close()
        print("SQL Server connection test: PASSED")

    print("\nTesting Data Warehouse connection...")
    dw_conn = connect_data_warehouse()
    if dw_conn:
        dw_conn.close()
        print("Data Warehouse connection test: PASSED")