import pandas as pd
import numpy as np
import pyodbc
from DatabaseConfig import DatabaseConfig, connect_sql_server, connect_data_warehouse
import create_dw


class etl:

    def __init__(self):
        print("=" * 50)
        print("INITIALISATION ETL NORTHWIND")
        print("=" * 50)

        # Connect to Northwind SQL Server
        print("\n1. Connecting to Northwind SQL...")
        self.source_conn = connect_sql_server()

        if self.source_conn is None:
            raise Exception("❌ ERROR: Failed connecting to Northwind SQL")
        print("   ✅ Connected to Northwind SQL")

        # Verify/create DW
        print("\n2. Verify/create DW...")
        try:
            if create_dw.create_datawarehouse():
                print("   ✅ DW verified/created")
                if create_dw.create_dw_schema():
                    print("   ✅ Schema DW verified/created")
                else:
                    print("   ⚠️  Schema DW not created (might already exist)")
            else:
                print("   ⚠️  DW not created (might already exist)")
        except Exception as e:
            print(f"   ⚠️  Error creation DW: {e}")

        # Connect to DW
        print("\n3. Connecting to DW...")
        self.dw_conn = connect_data_warehouse()

        if self.dw_conn is None:
            raise Exception("❌ ERROR: Failed connecting to DW")
        print("   ✅ Connected to DW")

        print("\n" + "=" * 50)
        print("✅ ALL CONNECTIONS ARE DONE")
        print("=" * 50)

    # HELPER FUNCTIONS
    def check_table_exists(self, table_name):
        try:
            cursor = self.dw_conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = ?
            """, table_name)
            exists = cursor.fetchone()[0] > 0
            cursor.close()
            return exists
        except Exception as e:
            print(f"⚠️ Error checking table {table_name}: {e}")
            return False

    def fill_dim_date(self, start_year=1990, end_year=2025):
        print("\nDIMENSION DATE")
        print("-" * 30)

        # Ensure table exists first!
        self._ensure_dimdate_table_exists()

        try:
            count = self.dw_conn.execute("SELECT COUNT(*) FROM DimDate").fetchone()[0]
            if count > 0:
                print(f" DimDate already has {count:,} dates")
                return pd.DataFrame()
        except:
            pass

        print(f" Creating dates from {start_year} to {end_year}...")

        dates = pd.date_range(start=f'{start_year}-01-01', end=f'{end_year}-12-31', freq='D')
        dim_date = pd.DataFrame({
            'DateKey': dates.strftime('%Y%m%d').astype(int),
            'Date': dates.date,
            'Year': dates.year,
            'Quarter': dates.quarter,
            'Month': dates.month,
            'Day': dates.day,
            'MonthName': dates.strftime('%B'),
            'DayOfWeek': dates.strftime('%A'),
            'IsWeekend': (dates.weekday >= 5).astype(int)
        })

        print(f" Loading {len(dim_date):,} dates into SQL Server...")

        cursor = self.dw_conn.cursor()
        cursor.fast_executemany = True

        data_to_insert = [
            (
                row.DateKey,
                row.Date,
                row.Year,
                row.Quarter,
                row.Month,
                row.Day,
                row.MonthName,
                row.DayOfWeek,
                row.IsWeekend
            )
            for row in dim_date.itertuples()
        ]

        cursor.executemany("""
            INSERT INTO DimDate 
            (DateKey, Date, Year, Quarter, Month, Day, MonthName, DayOfWeek, IsWeekend)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, data_to_insert)

        self.dw_conn.commit()
        cursor.close()

        print(f" DimDate created successfully: {len(dim_date):,} dates inserted")
        return dim_date

    def create_access_mapping(self):
        """Create mapping between Access IDs and names"""
        print("\n🗺️  CREATING ACCESS MAPPING")
        print("-" * 30)

        mapping = {
            'customers': {},  # CustomerID -> CompanyName
            'employees': {}  # EmployeeID -> FullName
        }

        # 1. Read original Access data for mapping
        try:
            access_conn_str = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={DatabaseConfig.ACCESS_DB_PATH};"
            access_conn = pyodbc.connect(access_conn_str)

            # Access Customers mapping
            customers_df = pd.read_sql("SELECT [ID], [Company] FROM [Customers]", access_conn)
            for _, row in customers_df.iterrows():
                customer_id = str(row['ID'])
                company_name = str(row['Company'])
                mapping['customers'][customer_id] = company_name

            # Access Employees mapping
            employees_df = pd.read_sql("SELECT [ID], [First Name], [Last Name] FROM [Employees]", access_conn)
            for _, row in employees_df.iterrows():
                employee_id = str(row['ID'])
                first_name = str(row['First Name'])
                last_name = str(row['Last Name'])
                full_name = f"{first_name} {last_name}"
                mapping['employees'][employee_id] = full_name

            access_conn.close()

            print(f"  ✅ Mapping created: {len(mapping['customers'])} customers, {len(mapping['employees'])} employees")

        except Exception as e:
            print(f"  ❌ Error creating mapping: {e}")

        return mapping

    def _ensure_dimcustomer_table_exists(self):
        """Ensure DimCustomer table exists"""
        try:
            cursor = self.dw_conn.cursor()
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='DimCustomer' AND xtype='U')
                BEGIN
                    CREATE TABLE DimCustomer (
                        CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
                        CustomerID VARCHAR(10) NOT NULL,
                        CompanyName VARCHAR(100) NOT NULL,
                        ContactName VARCHAR(100),
                        ContactTitle VARCHAR(100),
                        Address VARCHAR(200),
                        City VARCHAR(50),
                        Region VARCHAR(50),
                        PostalCode VARCHAR(20),
                        Country VARCHAR(50),
                        Phone VARCHAR(30),
                        SourceSystem VARCHAR(20),
                        UNIQUE(CustomerID, SourceSystem)
                    );
                END
            """)
            self.dw_conn.commit()
            cursor.close()
        except Exception as e:
            print(f"⚠️  Error creating DimCustomer: {e}")

    def _ensure_dimemployee_table_exists(self):
        """Ensure DimEmployee table exists"""
        try:
            cursor = self.dw_conn.cursor()
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='DimEmployee' AND xtype='U')
                BEGIN
                    CREATE TABLE DimEmployee (
                        EmployeeKey INT IDENTITY(1,1) PRIMARY KEY,
                        EmployeeID INT NOT NULL,
                        LastName VARCHAR(50) NOT NULL,
                        FirstName VARCHAR(50) NOT NULL,
                        Title VARCHAR(100),
                        TitleOfCourtesy VARCHAR(25),
                        BirthDate DATE,
                        HireDate DATE,
                        Address VARCHAR(200),
                        City VARCHAR(50),
                        Region VARCHAR(50),
                        PostalCode VARCHAR(20),
                        Country VARCHAR(50),
                        HomePhone VARCHAR(30),
                        ReportsTo INT,
                        SourceSystem VARCHAR(20),
                        UNIQUE(EmployeeID, SourceSystem)
                    );
                END
            """)
            self.dw_conn.commit()
            cursor.close()
        except Exception as e:
            print(f"⚠️  Error creating DimEmployee: {e}")

    def _ensure_factorders_table_exists(self):
        """Ensure FactOrders table exists"""
        try:
            cursor = self.dw_conn.cursor()
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='FactOrders' AND xtype='U')
                BEGIN
                    CREATE TABLE FactOrders (
                        FactOrderKey INT IDENTITY(1,1) PRIMARY KEY,
                        OrderID INT NOT NULL,
                        CustomerKey INT,
                        EmployeeKey INT,
                        OrderDateKey INT,
                        OrderDate DATE,
                        RequiredDate DATE,
                        ShippedDate DATE,
                        ShipVia INT,
                        Freight DECIMAL(10,2),
                        ShipName VARCHAR(100),
                        ShipAddress VARCHAR(200),
                        ShipCity VARCHAR(50),
                        ShipRegion VARCHAR(50),
                        ShipPostalCode VARCHAR(20),
                        ShipCountry VARCHAR(50),
                        TotalAmount DECIMAL(10,2),
                        IsDelivered BIT,
                        DeliveryDelayDays INT,
                        SourceSystem VARCHAR(20),
                        FOREIGN KEY (CustomerKey) REFERENCES DimCustomer(CustomerKey),
                        FOREIGN KEY (EmployeeKey) REFERENCES DimEmployee(EmployeeKey),
                        FOREIGN KEY (OrderDateKey) REFERENCES DimDate(DateKey)
                    );

                    -- Create indexes for performance
                    CREATE INDEX IX_FactOrders_OrderDateKey ON FactOrders(OrderDateKey);
                    CREATE INDEX IX_FactOrders_CustomerKey ON FactOrders(CustomerKey);
                    CREATE INDEX IX_FactOrders_EmployeeKey ON FactOrders(EmployeeKey);
                END
            """)
            self.dw_conn.commit()
            cursor.close()
            print("  ✅ FactOrders table verified/created")
        except Exception as e:
            print(f"  ❌ Error creating FactOrders: {e}")

    def _ensure_dimdate_table_exists(self):
        """Ensure DimDate table exists"""
        try:
            cursor = self.dw_conn.cursor()
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='DimDate' AND xtype='U')
                BEGIN
                    CREATE TABLE DimDate (
                        DateKey INT PRIMARY KEY,
                        Date DATE NOT NULL,
                        Year INT NOT NULL,
                        Quarter INT NOT NULL,
                        Month INT NOT NULL,
                        Day INT NOT NULL,
                        MonthName VARCHAR(20) NOT NULL,
                        DayOfWeek VARCHAR(20) NOT NULL,
                        IsWeekend BIT NOT NULL
                    );

                    -- Create indexes for performance
                    CREATE INDEX IX_DimDate_Date ON DimDate(Date);
                    CREATE INDEX IX_DimDate_Year ON DimDate(Year);
                    CREATE INDEX IX_DimDate_YearMonth ON DimDate(Year, Month);
                END
            """)
            self.dw_conn.commit()
            cursor.close()
            print("  ✅ DimDate table verified/created")
        except Exception as e:
            print(f"  ❌ Error creating DimDate: {e}")


    # EXTRACT FUNCTIONS - EXTRACT ONLY, NO TRANSFORMATION
    def extract_from_sql_server(self):
        print("\n📥 EXTRACT FROM SQL SERVER")
        print("-" * 30)

        queries = {
            'customers': """
                SELECT CustomerID, CompanyName, ContactName, ContactTitle, 
                       Address, City, Region, PostalCode, Country, Phone
                FROM Customers
                WHERE CustomerID IS NOT NULL
            """,
            'employees': """
                SELECT EmployeeID, LastName, FirstName, Title, TitleOfCourtesy,
                       BirthDate, HireDate, Address, City, Region, PostalCode,
                       Country, HomePhone, ReportsTo
                FROM Employees
                WHERE EmployeeID IS NOT NULL
            """,
            'orders': """
                SELECT o.OrderID, o.CustomerID, o.EmployeeID, 
                       o.OrderDate, o.RequiredDate, o.ShippedDate,
                       o.ShipVia, o.Freight, o.ShipName, o.ShipAddress,
                       o.ShipCity, o.ShipRegion, o.ShipPostalCode, o.ShipCountry,
                       SUM(od.Quantity * od.UnitPrice * (1 - od.Discount)) as TotalAmount
                FROM Orders o
                LEFT JOIN [Order Details] od ON o.OrderID = od.OrderID
                WHERE o.OrderID IS NOT NULL
                GROUP BY o.OrderID, o.CustomerID, o.EmployeeID, o.OrderDate, 
                         o.RequiredDate, o.ShippedDate, o.ShipVia, o.Freight,
                         o.ShipName, o.ShipAddress, o.ShipCity, o.ShipRegion,
                         o.ShipPostalCode, o.ShipCountry
                ORDER BY o.OrderID
            """
        }

        data = {}
        for name, query in queries.items():
            try:
                data[name] = pd.read_sql(query, self.source_conn)
                print(f"  ✅ {name}: {len(data[name])} rows")
            except Exception as e:
                print(f"  ❌ Error extracting {name}: {e}")
                data[name] = pd.DataFrame()

        return data

    def extract_from_access(self):
        """EXTRACT ONLY - No transformation in this function"""
        if not DatabaseConfig.ACCESS_DB_PATH:
            print("\nℹ️  No Access database configured")
            return {}

        print("\n📥 EXTRACT FROM ACCESS (RAW DATA ONLY)")
        print("-" * 30)

        try:
            access_conn_str = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={DatabaseConfig.ACCESS_DB_PATH};"
            access_conn = pyodbc.connect(access_conn_str)

            # Discover available tables
            cursor = access_conn.cursor()
            tables = cursor.tables(tableType='TABLE')
            table_list = []
            for table in tables:
                table_list.append(table.table_name)
            cursor.close()

            print(f"Available tables in Access: {table_list}")

            # Dictionary for raw extracted data
            raw_data = {}

            # Extract Customers (RAW - no transformation)
            try:
                print("  Extracting Customers (raw)...")
                customer_table = 'Customers'
                if customer_table not in table_list:
                    for table in table_list:
                        if 'customer' in table.lower():
                            customer_table = table
                            break

                query = f"SELECT * FROM [{customer_table}]"
                raw_data['customers_raw'] = pd.read_sql(query, access_conn)
                print(f"  ✅ Raw Customers: {len(raw_data['customers_raw'])} rows")
                print(f"    Columns: {list(raw_data['customers_raw'].columns)}")
            except Exception as e:
                print(f"  ❌ Error extracting Customers: {e}")
                raw_data['customers_raw'] = pd.DataFrame()

            # Extract Employees (RAW - no transformation)
            try:
                print("  Extracting Employees (raw)...")
                employee_table = 'Employees'
                if employee_table not in table_list:
                    for table in table_list:
                        if 'employee' in table.lower():
                            employee_table = table
                            break

                query = f"SELECT * FROM [{employee_table}]"
                raw_data['employees_raw'] = pd.read_sql(query, access_conn)
                print(f"  ✅ Raw Employees: {len(raw_data['employees_raw'])} rows")
            except Exception as e:
                print(f"  ❌ Error extracting Employees: {e}")
                raw_data['employees_raw'] = pd.DataFrame()

            # Extract Orders (RAW - no transformation)
            try:
                print("  Extracting Orders (raw)...")
                orders_table = 'Orders'
                if orders_table not in table_list:
                    for table in table_list:
                        if 'order' in table.lower() and 'detail' not in table.lower():
                            orders_table = table
                            break

                query = f"SELECT * FROM [{orders_table}]"
                raw_data['orders_raw'] = pd.read_sql(query, access_conn)
                print(f"  ✅ Raw Orders: {len(raw_data['orders_raw'])} rows")
            except Exception as e:
                print(f"  ❌ Error extracting Orders: {e}")
                raw_data['orders_raw'] = pd.DataFrame()

            # Extract Order Details (for TotalAmount calculation)
            try:
                print("  Extracting Order Details (raw)...")
                order_details_table = 'Order Details'
                if order_details_table not in table_list:
                    for table in table_list:
                        if 'order detail' in table.lower() or 'order_details' in table.lower():
                            order_details_table = table
                            break

                query = f"SELECT * FROM [{order_details_table}]"
                raw_data['order_details_raw'] = pd.read_sql(query, access_conn)
                print(f"  ✅ Raw Order Details: {len(raw_data['order_details_raw'])} rows")
            except Exception as e:
                print(f"  ⚠️  Error extracting Order Details: {e}")
                raw_data['order_details_raw'] = pd.DataFrame()

            access_conn.close()

            # Check if we got any data
            has_data = False
            for key, df in raw_data.items():
                if not df.empty:
                    has_data = True
                    break

            if not has_data:
                print("  ℹ️  No data extracted from Access")

            return raw_data

        except Exception as e:
            print(f"  ❌ Cannot access Access database: {e}")
            return {}



    # TRANSFORM FUNCTIONS - COMPLETE TRANSFORMATION FOR BOTH SQL AND ACCESS
    def transform_dim_customer(self, customers_df, source_name='SQL'):
        print(f"\n👥 TRANSFORM DIMCUSTOMER ({source_name})")
        print("-" * 30)

        if customers_df.empty:
            print("  ⚠️  No customer data")
            return pd.DataFrame()

        dim_customer = customers_df.copy()

        # Different column mapping for Access vs SQL
        if source_name == 'Access':
            # Access has different column names - map them
            column_mapping = {
                'ID': 'CustomerID',
                'Company': 'CompanyName',
                'Last Name': 'LastName',
                'First Name': 'FirstName',
                'Business Phone': 'Phone',
                'Address': 'Address',
                'City': 'City',
                'State/Province': 'Region',
                'ZIP/Postal Code': 'PostalCode',
                'Country/Region': 'Country'
            }

            # Try multiple naming variations
            for old_col, new_col in column_mapping.items():
                if old_col in dim_customer.columns:
                    dim_customer = dim_customer.rename(columns={old_col: new_col})

            # Create ContactName from separate name fields
            if 'FirstName' in dim_customer.columns and 'LastName' in dim_customer.columns:
                dim_customer['ContactName'] = dim_customer['FirstName'].fillna('') + ' ' + dim_customer[
                    'LastName'].fillna('')
                dim_customer['ContactName'] = dim_customer['ContactName'].str.strip()

            # Access doesn't have ContactTitle, add default
            if 'ContactTitle' not in dim_customer.columns:
                dim_customer['ContactTitle'] = 'Customer'

        else:  # SQL Server
            # SQL already has standard names
            column_mapping = {
                'CustomerID': 'CustomerID',
                'CompanyName': 'CompanyName',
                'ContactName': 'ContactName',
                'ContactTitle': 'ContactTitle',
                'Address': 'Address',
                'City': 'City',
                'Region': 'Region',
                'PostalCode': 'PostalCode',
                'Country': 'Country',
                'Phone': 'Phone'
            }

        # Rename columns based on mapping
        for old_col, new_col in column_mapping.items():
            if old_col in dim_customer.columns and new_col not in dim_customer.columns:
                dim_customer = dim_customer.rename(columns={old_col: new_col})

        # Define required columns for DimCustomer
        required_cols = [
            'CustomerID', 'CompanyName', 'ContactName', 'ContactTitle',
            'Address', 'City', 'Region', 'PostalCode', 'Country', 'Phone'
        ]

        # Add missing required columns as None
        for col in required_cols:
            if col not in dim_customer.columns:
                dim_customer[col] = None

        # Add source system tag
        dim_customer['SourceSystem'] = source_name

        # ACCESS-SPECIFIC TRANSFORMATIONS
        if source_name == 'Access':
            # Convert CustomerID to string and add ACC- prefix
            if 'CustomerID' in dim_customer.columns:
                dim_customer['CustomerID'] = pd.to_numeric(dim_customer['CustomerID'], errors='coerce')
                dim_customer['CustomerID'] = 'ACC-' + dim_customer['CustomerID'].fillna(0).astype(int).astype(str)

        # GENERAL CLEANING (applies to both SQL and Access)

        # Remove rows with null CustomerID
        if 'CustomerID' in dim_customer.columns:
            initial_count = len(dim_customer)
            dim_customer = dim_customer[dim_customer['CustomerID'].notna()]
            filtered_count = len(dim_customer)
            if filtered_count < initial_count:
                print(f"  ⚠️  {initial_count - filtered_count} rows filtered (CustomerID NULL)")

        # Fill nulls with appropriate defaults
        if 'Region' in dim_customer.columns:
            dim_customer['Region'] = dim_customer['Region'].fillna('Unknown')
        if 'PostalCode' in dim_customer.columns:
            dim_customer['PostalCode'] = dim_customer['PostalCode'].fillna('Unknown')
        if 'ContactTitle' in dim_customer.columns:
            dim_customer['ContactTitle'] = dim_customer['ContactTitle'].fillna('Unknown')

        # Convert all text columns to strings
        for col in dim_customer.columns:
            if dim_customer[col].dtype == 'object':
                dim_customer[col] = dim_customer[col].astype(str)

        # Keep only required columns
        available_required = [col for col in required_cols if col in dim_customer.columns]
        if available_required:
            dim_customer = dim_customer[available_required + ['SourceSystem']]

        print(f"  ✅ {len(dim_customer)} customers transformed")
        return dim_customer

    def transform_dim_employee(self, employees_df, source_name='SQL'):
        print(f"\n👨‍💼 TRANSFORM DIMEMPLOYEE ({source_name})")
        print("-" * 30)

        if employees_df.empty:
            print("  ⚠️  No employee data")
            return pd.DataFrame()

        dim_employee = employees_df.copy()

        # Different column mapping for Access vs SQL
        if source_name == 'Access':
            # Access has different column names
            column_mapping = {
                'ID': 'EmployeeID',
                'Last Name': 'LastName',
                'First Name': 'FirstName',
                'Job Title': 'Title',
                'Business Phone': 'HomePhone',
                'Address': 'Address',
                'City': 'City',
                'State/Province': 'Region',
                'ZIP/Postal Code': 'PostalCode',
                'Country/Region': 'Country'
            }

            for old_col, new_col in column_mapping.items():
                if old_col in dim_employee.columns:
                    dim_employee = dim_employee.rename(columns={old_col: new_col})

            # Access doesn't have these columns, add defaults
            if 'TitleOfCourtesy' not in dim_employee.columns:
                dim_employee['TitleOfCourtesy'] = 'Mr.'
            if 'BirthDate' not in dim_employee.columns:
                dim_employee['BirthDate'] = None
            if 'HireDate' not in dim_employee.columns:
                dim_employee['HireDate'] = None
            if 'ReportsTo' not in dim_employee.columns:
                dim_employee['ReportsTo'] = None

        else:  # SQL Server
            column_mapping = {
                'EmployeeID': 'EmployeeID',
                'LastName': 'LastName',
                'FirstName': 'FirstName',
                'Title': 'Title',
                'TitleOfCourtesy': 'TitleOfCourtesy',
                'BirthDate': 'BirthDate',
                'HireDate': 'HireDate',
                'Address': 'Address',
                'City': 'City',
                'Region': 'Region',
                'PostalCode': 'PostalCode',
                'Country': 'Country',
                'HomePhone': 'HomePhone',
                'ReportsTo': 'ReportsTo'
            }

        # Rename columns
        for old_col, new_col in column_mapping.items():
            if old_col in dim_employee.columns and new_col not in dim_employee.columns:
                dim_employee = dim_employee.rename(columns={old_col: new_col})

        # Define required columns
        required_cols = [
            'EmployeeID', 'LastName', 'FirstName', 'Title', 'TitleOfCourtesy',
            'BirthDate', 'HireDate', 'Address', 'City', 'Region', 'PostalCode',
            'Country', 'HomePhone', 'ReportsTo'
        ]

        # Add missing required columns
        for col in required_cols:
            if col not in dim_employee.columns:
                dim_employee[col] = None

        # Add source system tag
        dim_employee['SourceSystem'] = source_name

        # ACCESS-SPECIFIC TRANSFORMATIONS
        if source_name == 'Access':
            # Convert and prefix EmployeeID
            if 'EmployeeID' in dim_employee.columns:
                dim_employee['EmployeeID'] = pd.to_numeric(dim_employee['EmployeeID'], errors='coerce')
                dim_employee['EmployeeID'] = 1000 + dim_employee['EmployeeID'].fillna(0).astype(int)

        # GENERAL CLEANING

        # Remove rows with null EmployeeID
        if 'EmployeeID' in dim_employee.columns:
            initial_count = len(dim_employee)
            dim_employee = dim_employee[dim_employee['EmployeeID'].notna()]
            filtered_count = len(dim_employee)
            if filtered_count < initial_count:
                print(f"  ⚠️  {initial_count - filtered_count} rows filtered (EmployeeID NULL)")

        # Convert date columns
        date_cols = ['BirthDate', 'HireDate']
        for col in date_cols:
            if col in dim_employee.columns:
                dim_employee[col] = pd.to_datetime(dim_employee[col], errors='coerce')

        # Fill nulls
        if 'Region' in dim_employee.columns:
            dim_employee['Region'] = dim_employee['Region'].fillna('Unknown')
        if 'PostalCode' in dim_employee.columns:
            dim_employee['PostalCode'] = dim_employee['PostalCode'].fillna('Unknown')
        if 'Title' in dim_employee.columns:
            dim_employee['Title'] = dim_employee['Title'].fillna('Unknown')
        if 'TitleOfCourtesy' in dim_employee.columns:
            dim_employee['TitleOfCourtesy'] = dim_employee['TitleOfCourtesy'].fillna('Unknown')

        # Convert numeric columns
        if 'EmployeeID' in dim_employee.columns:
            dim_employee['EmployeeID'] = pd.to_numeric(dim_employee['EmployeeID'], errors='coerce')
            dim_employee = dim_employee[dim_employee['EmployeeID'].notna()]

        if 'ReportsTo' in dim_employee.columns:
            dim_employee['ReportsTo'] = pd.to_numeric(dim_employee['ReportsTo'], errors='coerce')

        # Convert text to strings
        for col in dim_employee.columns:
            if dim_employee[col].dtype == 'object':
                dim_employee[col] = dim_employee[col].astype(str)

        # Keep only required columns
        available_required = [col for col in required_cols if col in dim_employee.columns]
        if available_required:
            dim_employee = dim_employee[available_required + ['SourceSystem']]

        print(f"  ✅ {len(dim_employee)} employees transformed")
        return dim_employee

    def transform_fact_orders(self, orders_df, source_name='SQL'):
        print(f"\n📦 TRANSFORM FACTORDERS ({source_name})")
        print("-" * 30)

        if orders_df.empty:
            print("  ⚠️  No order data")
            return pd.DataFrame()

        fact_orders = orders_df.copy()

        # For Access, we might need to calculate TotalAmount from Order Details
        if source_name == 'Access' and 'TotalAmount' not in fact_orders.columns:
            print("  ℹ️  Calculating TotalAmount from Order Details...")
            # This would require the order_details_raw data to be passed or accessible
            # For now, we'll add a placeholder

        # Different column mapping for Access vs SQL
        if source_name == 'Access':
            column_mapping = {
                'Order ID': 'OrderID',
                'ID': 'OrderID',
                'Customer': 'CustomerID',
                'Employee': 'EmployeeID',
                'Order Date': 'OrderDate',
                'Required Date': 'RequiredDate',
                'Shipped Date': 'ShippedDate',
                'Shipping Fee': 'Freight',
                'Ship Fee': 'Freight',
                'Ship Name': 'ShipName',
                'Ship Address': 'ShipAddress',
                'Ship City': 'ShipCity',
                'Ship State/Province': 'ShipRegion',
                'Ship Region': 'ShipRegion',
                'Ship ZIP/Postal Code': 'ShipPostalCode',
                'Ship Postal Code': 'ShipPostalCode',
                'Ship Country/Region': 'ShipCountry',
                'Ship Country': 'ShipCountry'
            }

            for old_col, new_col in column_mapping.items():
                if old_col in fact_orders.columns:
                    fact_orders = fact_orders.rename(columns={old_col: new_col})

            # Access might not have these
            if 'ShipVia' not in fact_orders.columns:
                fact_orders['ShipVia'] = 1
            if 'TotalAmount' not in fact_orders.columns:
                fact_orders['TotalAmount'] = 0.0

        else:  # SQL Server
            column_mapping = {
                'OrderID': 'OrderID',
                'CustomerID': 'CustomerID',
                'EmployeeID': 'EmployeeID',
                'OrderDate': 'OrderDate',
                'RequiredDate': 'RequiredDate',
                'ShippedDate': 'ShippedDate',
                'ShipVia': 'ShipVia',
                'Freight': 'Freight',
                'ShipName': 'ShipName',
                'ShipAddress': 'ShipAddress',
                'ShipCity': 'ShipCity',
                'ShipRegion': 'ShipRegion',
                'ShipPostalCode': 'ShipPostalCode',
                'ShipCountry': 'ShipCountry',
                'TotalAmount': 'TotalAmount'
            }

        # Rename columns
        for old_col, new_col in column_mapping.items():
            if old_col in fact_orders.columns and new_col not in fact_orders.columns:
                fact_orders = fact_orders.rename(columns={old_col: new_col})

        # Define required columns
        required_cols = [
            'OrderID', 'CustomerID', 'EmployeeID', 'OrderDate',
            'RequiredDate', 'ShippedDate', 'ShipVia', 'Freight',
            'ShipName', 'ShipAddress', 'ShipCity', 'ShipRegion',
            'ShipPostalCode', 'ShipCountry', 'TotalAmount'
        ]

        # Add missing required columns
        for col in required_cols:
            if col not in fact_orders.columns:
                fact_orders[col] = None

        # Convert date columns
        date_cols = ['OrderDate', 'RequiredDate', 'ShippedDate']
        for col in date_cols:
            if col in fact_orders.columns:
                fact_orders[col] = pd.to_datetime(fact_orders[col], errors='coerce')

        # Calculate derived columns
        fact_orders['IsDelivered'] = fact_orders['ShippedDate'].notna().astype(int)

        if 'ShippedDate' in fact_orders.columns and 'RequiredDate' in fact_orders.columns:
            fact_orders['DeliveryDelayDays'] = np.where(
                fact_orders['ShippedDate'].notna() & fact_orders['RequiredDate'].notna(),
                (fact_orders['ShippedDate'] - fact_orders['RequiredDate']).dt.days,
                None
            )
        else:
            fact_orders['DeliveryDelayDays'] = None

        # Add source system tag
        fact_orders['SourceSystem'] = source_name

        # ==================================================
        # ACCESS-SPECIFIC TRANSFORMATIONS WITH FIX FOR ID 0
        # ==================================================
        if source_name == 'Access':
            # Process CustomerID - FIX: Only convert valid IDs (> 0)
            if 'CustomerID' in fact_orders.columns:
                try:
                    # Convert to numeric, handle errors
                    fact_orders['CustomerID'] = pd.to_numeric(fact_orders['CustomerID'], errors='coerce')

                    # Debug info
                    invalid_customers = fact_orders['CustomerID'].isna() | (fact_orders['CustomerID'] <= 0)
                    valid_customers = ~invalid_customers

                    if invalid_customers.any():
                        print(
                            f"  ⚠️  Found {invalid_customers.sum()} Access orders with invalid CustomerID (0, NaN, or negative)")
                        # Set invalid CustomerIDs to None
                        fact_orders.loc[invalid_customers, 'CustomerID'] = None

                    # Only convert valid IDs (> 0) to ACC- prefix format
                    if valid_customers.any():
                        fact_orders.loc[valid_customers, 'CustomerID'] = 'ACC-' + fact_orders.loc[
                            valid_customers, 'CustomerID'].astype(int).astype(str)

                except Exception as e:
                    print(f"  ⚠️  Error processing Access CustomerID: {e}")
                    # Keep as is if conversion fails
                    pass

            # Process EmployeeID - FIX: Only convert valid IDs (> 0)
            if 'EmployeeID' in fact_orders.columns:
                try:
                    # Convert to numeric, handle errors
                    fact_orders['EmployeeID'] = pd.to_numeric(fact_orders['EmployeeID'], errors='coerce')

                    # Debug info
                    invalid_employees = fact_orders['EmployeeID'].isna() | (fact_orders['EmployeeID'] <= 0)
                    valid_employees = ~invalid_employees

                    if invalid_employees.any():
                        print(
                            f"  ⚠️  Found {invalid_employees.sum()} Access orders with invalid EmployeeID (0, NaN, or negative)")
                        # Set invalid EmployeeIDs to None
                        fact_orders.loc[invalid_employees, 'EmployeeID'] = None

                    # Only convert valid IDs (> 0) to 1000+ format
                    if valid_employees.any():
                        fact_orders.loc[valid_employees, 'EmployeeID'] = 1000 + fact_orders.loc[
                            valid_employees, 'EmployeeID'].astype(int)

                except Exception as e:
                    print(f"  ⚠️  Error processing Access EmployeeID: {e}")
                    # Keep as is if conversion fails
                    pass

            # Debug: Show sample of problematic records
            if 'CustomerID' in fact_orders.columns and 'EmployeeID' in fact_orders.columns:
                invalid_records = fact_orders[
                    (fact_orders['CustomerID'].isna()) |
                    (fact_orders['EmployeeID'].isna()) |
                    (fact_orders['CustomerID'] == 'ACC-0') |
                    (fact_orders['EmployeeID'] == 1000)
                    ]

                if not invalid_records.empty:
                    print(f"  ⚠️  Sample problematic orders (showing first 5):")
                    for idx, row in invalid_records.head().iterrows():
                        print(f"    Order {row.get('OrderID', 'N/A')}: "
                              f"CustomerID={row.get('CustomerID', 'N/A')}, "
                              f"EmployeeID={row.get('EmployeeID', 'N/A')}")

        # Convert numeric columns
        if 'Freight' in fact_orders.columns:
            fact_orders['Freight'] = pd.to_numeric(fact_orders['Freight'], errors='coerce').fillna(0)
        if 'TotalAmount' in fact_orders.columns:
            fact_orders['TotalAmount'] = pd.to_numeric(fact_orders['TotalAmount'], errors='coerce').fillna(0)
        if 'ShipVia' in fact_orders.columns:
            fact_orders['ShipVia'] = pd.to_numeric(fact_orders['ShipVia'], errors='coerce').fillna(1)

        # Convert text to strings
        for col in fact_orders.columns:
            if fact_orders[col].dtype == 'object':
                fact_orders[col] = fact_orders[col].astype(str)

        # Add calculated columns to keep list
        keep_cols = required_cols + ['IsDelivered', 'DeliveryDelayDays', 'SourceSystem']
        available_cols = [col for col in keep_cols if col in fact_orders.columns]
        if available_cols:
            fact_orders = fact_orders[available_cols]

        print(f"  ✅ {len(fact_orders)} orders transformed")

        # Final validation check
        if source_name == 'Access':
            invalid_count = fact_orders[
                (fact_orders['CustomerID'].isna()) |
                (fact_orders['EmployeeID'].isna())
                ].shape[0]
            if invalid_count > 0:
                print(
                    f"  ℹ️  {invalid_count} Access orders have NULL CustomerID/EmployeeID (will be loaded with NULL keys)")

        return fact_orders



    #LOAD FUNCTIONS
    def load_dimensions_to_dw(self, dim_customer, dim_employee):
        print("\n📤 LOADING DIMENSIONS")
        print("-" * 30)

        if self.dw_conn is None:
            print("  ❌ No connection to DW")
            return

        self._ensure_dimcustomer_table_exists()
        self._ensure_dimemployee_table_exists()

        # Load customers
        if not dim_customer.empty:
            print("  📋 Loading DimCustomer...")
            try:
                existing_query = "SELECT CustomerID, SourceSystem FROM DimCustomer"
                existing_customers = pd.read_sql(existing_query, self.dw_conn)

                # Filter out existing customers
                if not existing_customers.empty:
                    dim_customer['composite_key'] = dim_customer['CustomerID'].astype(str) + '_' + dim_customer[
                        'SourceSystem'].astype(str)
                    existing_customers['composite_key'] = existing_customers['CustomerID'].astype(str) + '_' + \
                                                          existing_customers['SourceSystem'].astype(str)

                    new_customers = dim_customer[
                        ~dim_customer['composite_key'].isin(existing_customers['composite_key'])]
                    dim_customer = new_customers.drop('composite_key', axis=1, errors='ignore')

                # Insert new customers
                if not dim_customer.empty:
                    cursor = self.dw_conn.cursor()
                    inserted_count = 0

                    for _, row in dim_customer.iterrows():
                        try:
                            customer_id = str(row.get('CustomerID', '')) if pd.notna(row.get('CustomerID')) else ''

                            if not customer_id:
                                continue

                            cursor.execute("""
                                INSERT INTO DimCustomer (CustomerID, CompanyName, ContactName, ContactTitle, 
                                Address, City, Region, PostalCode, Country, Phone, SourceSystem)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                                           customer_id,
                                           str(row.get('CompanyName', '')) if pd.notna(row.get('CompanyName')) else '',
                                           str(row.get('ContactName', '')) if pd.notna(row.get('ContactName')) else '',
                                           str(row.get('ContactTitle', '')) if pd.notna(
                                               row.get('ContactTitle')) else '',
                                           str(row.get('Address', '')) if pd.notna(row.get('Address')) else '',
                                           str(row.get('City', '')) if pd.notna(row.get('City')) else '',
                                           str(row.get('Region', '')) if pd.notna(row.get('Region')) else '',
                                           str(row.get('PostalCode', '')) if pd.notna(row.get('PostalCode')) else '',
                                           str(row.get('Country', '')) if pd.notna(row.get('Country')) else '',
                                           str(row.get('Phone', '')) if pd.notna(row.get('Phone')) else '',
                                           str(row.get('SourceSystem', 'Unknown')) if pd.notna(
                                               row.get('SourceSystem')) else 'Unknown')

                            inserted_count += 1

                        except Exception as row_error:
                            print(f"    ⚠️  Row error {_}: {row_error}")
                            continue

                    self.dw_conn.commit()
                    cursor.close()

                    print(f"    ✅ {inserted_count} new customers added")
                else:
                    print("    ℹ️  All customers already exist")

            except Exception as e:
                print(f"    ❌ Error loading DimCustomer: {e}")
        else:
            print("  ℹ️  No customers to load")

        # Load employees
        if not dim_employee.empty:
            print("  📋 Loading DimEmployee...")
            try:
                existing_query = "SELECT EmployeeID, SourceSystem FROM DimEmployee"
                existing_employees = pd.read_sql(existing_query, self.dw_conn)

                # Filter out existing employees
                if not existing_employees.empty:
                    dim_employee['composite_key'] = dim_employee['EmployeeID'].astype(str) + '_' + dim_employee[
                        'SourceSystem'].astype(str)
                    existing_employees['composite_key'] = existing_employees['EmployeeID'].astype(str) + '_' + \
                                                          existing_employees['SourceSystem'].astype(str)

                    new_employees = dim_employee[
                        ~dim_employee['composite_key'].isin(existing_employees['composite_key'])]
                    dim_employee = new_employees.drop('composite_key', axis=1, errors='ignore')

                # Insert new employees
                if not dim_employee.empty:
                    cursor = self.dw_conn.cursor()
                    inserted_count = 0

                    for _, row in dim_employee.iterrows():
                        try:
                            employee_id = int(row.get('EmployeeID', 0)) if pd.notna(row.get('EmployeeID')) else 0

                            if employee_id == 0:
                                continue

                            # Handle ReportsTo field
                            reports_to = row.get('ReportsTo')
                            if pd.isna(reports_to):
                                reports_to = None
                            else:
                                reports_to = int(reports_to)

                            cursor.execute("""
                                INSERT INTO DimEmployee (EmployeeID, LastName, FirstName, Title, 
                                TitleOfCourtesy, BirthDate, HireDate, Address, City, Region, 
                                PostalCode, Country, HomePhone, ReportsTo, SourceSystem)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                                           employee_id,
                                           str(row.get('LastName', '')) if pd.notna(row.get('LastName')) else '',
                                           str(row.get('FirstName', '')) if pd.notna(row.get('FirstName')) else '',
                                           str(row.get('Title', '')) if pd.notna(row.get('Title')) else '',
                                           str(row.get('TitleOfCourtesy', '')) if pd.notna(
                                               row.get('TitleOfCourtesy')) else '',
                                           row.get('BirthDate') if pd.notna(row.get('BirthDate')) else None,
                                           row.get('HireDate') if pd.notna(row.get('HireDate')) else None,
                                           str(row.get('Address', '')) if pd.notna(row.get('Address')) else '',
                                           str(row.get('City', '')) if pd.notna(row.get('City')) else '',
                                           str(row.get('Region', '')) if pd.notna(row.get('Region')) else '',
                                           str(row.get('PostalCode', '')) if pd.notna(row.get('PostalCode')) else '',
                                           str(row.get('Country', '')) if pd.notna(row.get('Country')) else '',
                                           str(row.get('HomePhone', '')) if pd.notna(row.get('HomePhone')) else '',
                                           reports_to,
                                           str(row.get('SourceSystem', 'Unknown')) if pd.notna(
                                               row.get('SourceSystem')) else 'Unknown')

                            inserted_count += 1

                        except Exception as row_error:
                            print(f"    ⚠️  Row error {_}: {row_error}")
                            continue

                    self.dw_conn.commit()
                    cursor.close()

                    print(f"    ✅ {inserted_count} new employees added")
                else:
                    print("    ℹ️  All employees already exist")

            except Exception as e:
                print(f"    ❌ Error loading DimEmployee: {e}")
        else:
            print("  ℹ️  No employees to load")

    def load_facts_to_dw(self, fact_orders):
        print("\n📤 LOADING FACTS")
        print("-" * 30)

        if self.dw_conn is None or fact_orders.empty:
            print("  ℹ️  No data to load")
            return

        # Create Access mapping
        access_mapping = self.create_access_mapping()

        # Verify/create table
        self._ensure_factorders_table_exists()

        print("  🔍 Intelligent dimension key lookup...")

        try:
            cursor = self.dw_conn.cursor()

            # Filter existing orders
            existing_query = "SELECT OrderID, SourceSystem FROM FactOrders"
            existing_orders = pd.read_sql(existing_query, self.dw_conn)

            if not existing_orders.empty:
                fact_orders['composite_key'] = fact_orders['OrderID'].astype(str) + '_' + fact_orders[
                    'SourceSystem'].astype(str)
                existing_orders['composite_key'] = existing_orders['OrderID'].astype(str) + '_' + existing_orders[
                    'SourceSystem'].astype(str)

                new_orders = fact_orders[~fact_orders['composite_key'].isin(existing_orders['composite_key'])]
                fact_orders = new_orders.drop('composite_key', axis=1, errors='ignore')

            if fact_orders.empty:
                print("  ℹ️  All orders already exist")
                return

            # Prepare DateKey
            fact_orders_with_keys = fact_orders.copy()
            if 'OrderDate' in fact_orders_with_keys.columns:
                fact_orders_with_keys['OrderDate'] = pd.to_datetime(fact_orders_with_keys['OrderDate'], errors='coerce')
                fact_orders_with_keys['OrderDateKey'] = fact_orders_with_keys['OrderDate'].dt.strftime('%Y%m%d').astype(
                    'Int64')

            # Insert with intelligent lookup
            inserted_count = 0
            error_count = 0

            for idx, row in fact_orders_with_keys.iterrows():
                try:
                    source_system = row.get('SourceSystem', 'SQL')
                    order_id = int(row.get('OrderID', 0)) if pd.notna(row.get('OrderID')) else 0

                    if order_id == 0:
                        continue

                    # DateKey (MANDATORY)
                    order_date = row.get('OrderDate')
                    order_date_key = None
                    if pd.notna(order_date):
                        try:
                            order_date_key = int(pd.to_datetime(order_date).strftime('%Y%m%d'))
                        except:
                            pass

                    if order_date_key is None:
                        print(f"    ⚠️  Order {order_id} skipped: no OrderDate")
                        continue

                    # CustomerKey - INTELLIGENT LOOKUP
                    customer_key = None
                    customer_id = row.get('CustomerID')

                    if source_system == 'SQL':
                        # For SQL Server
                        if pd.notna(customer_id):
                            cursor.execute("""
                                SELECT TOP 1 CustomerKey FROM DimCustomer 
                                WHERE CustomerID = ? AND SourceSystem = 'SQL'
                            """, (str(customer_id),))
                            result = cursor.fetchone()
                            if result:
                                customer_key = result[0]
                            else:
                                print(f"    ⚠️  SQL CustomerID {customer_id} not found")

                    elif source_system == 'Access':
                        # For Access
                        if pd.notna(customer_id):
                            # 1. Try with ACC-XX format
                            access_customer_id = f"ACC-{customer_id}"
                            cursor.execute("""
                                SELECT TOP 1 CustomerKey FROM DimCustomer 
                                WHERE CustomerID = ? AND SourceSystem = 'Access'
                            """, (access_customer_id,))
                            result = cursor.fetchone()

                            if result:
                                customer_key = result[0]
                            else:
                                # 2. Search by company name via mapping
                                company_name = access_mapping['customers'].get(str(customer_id))
                                if company_name:
                                    cursor.execute("""
                                        SELECT TOP 1 CustomerKey FROM DimCustomer 
                                        WHERE CompanyName LIKE ? AND SourceSystem = 'Access'
                                    """, (f"%{company_name}%",))
                                    result = cursor.fetchone()
                                    if result:
                                        customer_key = result[0]
                                else:
                                    print(f"    ⚠️  Access CustomerID {customer_id} not found")

                    # EmployeeKey - INTELLIGENT LOOKUP
                    employee_key = None
                    employee_id = row.get('EmployeeID')

                    if source_system == 'SQL':
                        # For SQL Server
                        if pd.notna(employee_id):
                            try:
                                emp_id = int(employee_id)
                                cursor.execute("""
                                    SELECT TOP 1 EmployeeKey FROM DimEmployee 
                                    WHERE EmployeeID = ? AND SourceSystem = 'SQL'
                                """, (emp_id,))
                                result = cursor.fetchone()
                                if result:
                                    employee_key = result[0]
                                else:
                                    print(f"    ⚠️  SQL EmployeeID {employee_id} not found")
                            except:
                                pass

                    elif source_system == 'Access':
                        # For Access
                        if pd.notna(employee_id):
                            try:
                                # 1. Try with 1000 + ID
                                access_employee_id = 1000 + int(employee_id)
                                cursor.execute("""
                                    SELECT TOP 1 EmployeeKey FROM DimEmployee 
                                    WHERE EmployeeID = ? AND SourceSystem = 'Access'
                                """, (access_employee_id,))
                                result = cursor.fetchone()

                                if result:
                                    employee_key = result[0]
                                else:
                                    # 2. Search by name via mapping
                                    full_name = access_mapping['employees'].get(str(employee_id))
                                    if full_name:
                                        # Try different name formats
                                        cursor.execute("""
                                            SELECT TOP 1 EmployeeKey FROM DimEmployee 
                                            WHERE (FirstName + ' ' + LastName LIKE ? 
                                                   OR LastName + ', ' + FirstName LIKE ?)
                                            AND SourceSystem = 'Access'
                                        """, (f"%{full_name}%", f"%{full_name}%"))
                                        result = cursor.fetchone()
                                        if result:
                                            employee_key = result[0]
                                    else:
                                        print(f"    ⚠️  Access EmployeeID {employee_id} not found")
                            except:
                                pass

                    # VALIDATION: If no keys found, we still insert with NULL
                    # But show a warning
                    if customer_key is None:
                        print(f"    ℹ️  Order {order_id}: CustomerKey = NULL (ID: {customer_id})")
                    if employee_key is None:
                        print(f"    ℹ️  Order {order_id}: EmployeeKey = NULL (ID: {employee_id})")

                    # Insert the order
                    cursor.execute("""
                        INSERT INTO FactOrders (
                            OrderID, CustomerKey, EmployeeKey, OrderDateKey,
                            OrderDate, ShippedDate, ShipVia, Freight,
                            ShipName, ShipAddress, ShipCity, ShipRegion,
                            ShipPostalCode, ShipCountry, TotalAmount,
                            IsDelivered, SourceSystem
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                                   order_id,
                                   customer_key,  # Can be NULL
                                   employee_key,  # Can be NULL
                                   order_date_key,
                                   order_date if pd.notna(order_date) else None,
                                   row.get('ShippedDate') if pd.notna(row.get('ShippedDate')) else None,
                                   int(row.get('ShipVia', 0)) if pd.notna(row.get('ShipVia')) else 0,
                                   float(row.get('Freight', 0)) if pd.notna(row.get('Freight')) else 0.0,
                                   str(row.get('ShipName', '')) if pd.notna(row.get('ShipName')) else '',
                                   str(row.get('ShipAddress', '')) if pd.notna(row.get('ShipAddress')) else '',
                                   str(row.get('ShipCity', '')) if pd.notna(row.get('ShipCity')) else '',
                                   str(row.get('ShipRegion', '')) if pd.notna(row.get('ShipRegion')) else '',
                                   str(row.get('ShipPostalCode', '')) if pd.notna(row.get('ShipPostalCode')) else '',
                                   str(row.get('ShipCountry', '')) if pd.notna(row.get('ShipCountry')) else '',
                                   float(row.get('TotalAmount', 0)) if pd.notna(row.get('TotalAmount')) else 0.0,
                                   int(row.get('IsDelivered', 0)) if pd.notna(row.get('IsDelivered')) else 0,
                                   str(source_system)
                                   )

                    inserted_count += 1

                    if inserted_count % 20 == 0:
                        print(f"    {inserted_count} orders inserted...")

                except Exception as row_error:
                    error_count += 1
                    if error_count <= 10:
                        print(f"    ⚠️  Row error {idx}: {str(row_error)[:80]}")
                    continue

            self.dw_conn.commit()
            cursor.close()

            print(f"\n  ✅ {inserted_count} orders loaded into FactOrders")
            print(f"  ℹ️  Summary:")
            print(f"    - Orders with CustomerKey: {inserted_count - error_count}")
            print(f"    - Orders with EmployeeKey: {inserted_count - error_count}")
            if error_count > 0:
                print(f"    - Errors: {error_count}")

        except Exception as e:
            print(f"  ❌ Loading error: {e}")
            import traceback
            traceback.print_exc()

    #SUMMARY
    def show_summary(self):
        print("\n📊 DATA WAREHOUSE SUMMARY")
        print("-" * 30)

        if self.dw_conn is None:
            print("❌ No connection to DW")
            return

        tables = ['DimDate', 'DimCustomer', 'DimEmployee', 'FactOrders']
        for table in tables:
            try:
                cursor = self.dw_conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                cursor.close()
                print(f"  {table}: {count} rows")
            except Exception as e:
                print(f"  {table}: TABLE NOT AVAILABLE")


    def run_full_etl(self):
        print("\n" + "=" * 50)
        print("🚀 FULL ETL ")
        print("=" * 50)

        try:
            # ensure ALL tables exist
            self._ensure_dimdate_table_exists()
            self._ensure_dimcustomer_table_exists()
            self._ensure_dimemployee_table_exists()
            self._ensure_factorders_table_exists()

            # create / fill DimDate
            self.fill_dim_date(1990, 2025)

            # extract from sql server
            sql_data = self.extract_from_sql_server()

            # Transform SQL data
            dim_customer_sql = self.transform_dim_customer(sql_data.get('customers', pd.DataFrame()), 'SQL')
            dim_employee_sql = self.transform_dim_employee(sql_data.get('employees', pd.DataFrame()), 'SQL')
            fact_orders_sql = self.transform_fact_orders(sql_data.get('orders', pd.DataFrame()), 'SQL')


            # extract from access
            access_data = self.extract_from_access()


            # Transform Access data
            if access_data:
                dim_customer_acc = self.transform_dim_customer(
                    access_data.get('customers_raw', pd.DataFrame()), 'Access'
                )
                dim_employee_acc = self.transform_dim_employee(
                    access_data.get('employees_raw', pd.DataFrame()), 'Access'
                )
                fact_orders_acc = self.transform_fact_orders(
                    access_data.get('orders_raw', pd.DataFrame()), 'Access'
                )



                # Combine SQL and Access data
                dim_customer = pd.concat([dim_customer_sql, dim_customer_acc], ignore_index=True)
                dim_employee = pd.concat([dim_employee_sql, dim_employee_acc], ignore_index=True)
                fact_orders = pd.concat([fact_orders_sql, fact_orders_acc], ignore_index=True)
            else:
                dim_customer = dim_customer_sql
                dim_employee = dim_employee_sql
                fact_orders = fact_orders_sql


            # Load dimensions and facts
            self.load_dimensions_to_dw(dim_customer, dim_employee)
            self.load_facts_to_dw(fact_orders)

            # Save for dashboard
            print("\n🎯 PREPARING FOR DASHBOARD")
            print("-" * 30)

            try:
                import os
                if not os.path.exists('data'):
                    os.makedirs('data')
                fact_orders.to_csv('data/fact_orders_transformed.csv', index=False)
                print("  ✅ Data saved to data/fact_orders_transformed.csv")
            except Exception as e:
                print(f"  ⚠️  Cannot save data: {e}")


            # Show summary
            self.show_summary()

            print("\n" + "=" * 50)
            print("🎉 ETL PROCESS COMPLETED SUCCESSFULLY!")
            print("=" * 50)

        except Exception as e:
            print(f"\n❌ CRITICAL ERROR IN ETL: {e}")
            raise



# MAIN EXECUTION
if __name__ == "__main__":
    try:
        print("🚀 STARTING ETL PROCESS")
        print("=" * 50)

        etl_processor = etl()
        etl_processor.run_full_etl()

    except Exception as e:
        print(f"\n❌ FATAL ERROR: {e}")
        import traceback

        traceback.print_exc()
    finally:
        print("\n" + "=" * 50)
        print("🏁 ETL PROCESS ENDED")
        print("=" * 50)