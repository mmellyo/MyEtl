
import pandas as pd
import numpy as np
import pyodbc

from DatabaseConfig import DatabaseConfig, connect_sql_server, connect_data_werehouse
import create_dw



class etl :

    # connect to Northwind SQLserver
    # verify / create dw
    # Connect to dw

    def __init__(self):
        #dubug
        print("=" * 50)
        print("INITIALISATION ETL NORTHWIND")
        print("=" * 50)



        # connect to Northwind SQLserver
        print("\n1. connecting to Northwind SQL...")
        self.source_conn = connect_sql_server()

        if self.source_conn is None:
            raise Exception("❌ ERROR : failed connecting to Northwind SQL")
        print("   ✅ connected to Northwind SQL")




        # verify / create dw
        print("\n2. Verify / create dw ...")
        try:
            if create_dw.create_datawarehouse():
                print("   ✅  DW Verified / created ")
                if create_dw.create_dw_schema():
                    print("   ✅ Schema DW  Verified / created")
                else:
                    print("   ⚠️  Schema DW not created (might alrd exist)")
            else:
                print("   ⚠️   DW not created (might alrd exist)")
        except Exception as e:
            print(f"   ⚠️  Error creation DW: {e}")



        # Connect to dw
        print("\n3. Connecting to dw...")
        self.dw_conn = connect_data_werehouse()

        if self.dw_conn is None:
            raise Exception("❌ ERROR : failed connecting to DW")
        print("   ✅ connected to DW")



        # Create engine SQLAlchemy for pandas to_sql (OPTIONNEL)
        #print("\n4. Initialisation SQLAlchemy (optionnel)...")
        #self.dw_engine = None  # On désactive SQLAlchemy pour éviter les erreurs
       # print("   ℹ️  Utilisation pyodbc direct uniquement")

        print("\n" + "=" * 50)
        print("✅ ALL CONNECTIONS ARE DONE")
        print("=" * 50)


    #HELPER FUNCTIONS
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

        # Verify if alrdy filled
        try:
            count = self.dw_conn.execute("SELECT COUNT(*) FROM DimDate").fetchone()[0]
            if count > 0:
                print(f" DimDate has alrdy {count:,} dates")
                return pd.DataFrame()
        except:
            pass  # table not exist or inaccessible → continue

        #create dates
        print(f" create dates from {start_year} until {end_year}...")

        dates = pd.date_range(start=f'{start_year}-01-01', end=f'{end_year}-12-31', freq='D')
        dim_date = pd.DataFrame({
            'DateKey': dates.strftime('%Y%m%d').astype(int),
            'Date': dates.date,  # ← datetime.date
            'Year': dates.year,
            'Quarter': dates.quarter,
            'Month': dates.month,
            'Day': dates.day,
            'MonthName': dates.strftime('%B'),
            'DayOfWeek': dates.strftime('%A'),
            'IsWeekend': (dates.weekday >= 5).astype(int)
        })

        print(f" loading {len(dim_date):,} dates in SQL Server...")

        cursor = self.dw_conn.cursor()
        cursor.fast_executemany = True  #

        # data as a tuples list
        data_to_insert = [
            (
                row.DateKey,
                row.Date,  # datetime.date
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

        print(f" DimDate created succefully : {len(dim_date):,} dates inserted")
        return dim_date


    #EXTRACT FUNCTIONS
    def extract_from_sql_server(self):
        print("\n📥 extract from sql server")
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
                print(f"  ✅ {name}: {len(data[name])} lignes")
            except Exception as e:
                print(f"  ❌ Error extraction {name}: {e}")
                data[name] = pd.DataFrame()

        return data

    def extract_from_access(self):
        #   1: Check if Access database path is configured
        if not DatabaseConfig.ACCESS_DB_PATH:
            print("\nℹ️  No Access database configured")
            return {}

        print("\n📥 EXTRACTION FROM ACCESS")
        print("-" * 30)

        try:
            #   2: Connect to Access database
            access_conn_str = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={DatabaseConfig.ACCESS_DB_PATH};"
            access_conn = pyodbc.connect(access_conn_str)

            #   3: Discover available tables in Access
            cursor = access_conn.cursor()
            tables = cursor.tables(tableType='TABLE')
            table_list = []
            for table in tables:
                table_list.append(table.table_name)
            cursor.close()

            print(f"Available tables in Access: {table_list}")

            # Dictionary to store extracted data
            access_data = {}

            #   4: Extract and transform Customers table
            try:
                print("  Extracting Customers...")
                customer_table = 'Customers'
                # Find customer table (handles different naming)
                if customer_table not in table_list:
                    for table in table_list:
                        if 'customer' in table.lower():
                            customer_table = table
                            break

                # Extract data
                query = f"SELECT * FROM [{customer_table}] WHERE [ID] IS NOT NULL"
                customers_df = pd.read_sql(query, access_conn)
                print(f"  ✅ Table {customer_table}: {len(customers_df)} rows")

                # Show columns for debugging
                print(f"    Columns: {list(customers_df.columns)}")

                #   4a: Rename Access columns to standard names
                customers_df = customers_df.rename(columns={
                    'ID': 'CustomerID',
                    'Company': 'CompanyName',
                    'Last Name': 'ContactLastName',
                    'First Name': 'ContactFirstName',
                    'Address': 'Address',
                    'City': 'City',
                    'State/Province': 'Region',
                    'ZIP/Postal Code': 'PostalCode',
                    'Country/Region': 'Country',
                    'Business Phone': 'Phone'
                })

                #  4b: Combine first and last name into ContactName
                if 'ContactFirstName' in customers_df.columns and 'ContactLastName' in customers_df.columns:
                    customers_df['ContactName'] = customers_df['ContactFirstName'] + ' ' + customers_df[
                        'ContactLastName']

                #   4c: Keep only necessary columns for DimCustomer
                required_cols = ['CustomerID', 'CompanyName', 'ContactName', 'Address',
                                 'City', 'Region', 'PostalCode', 'Country', 'Phone']
                available_cols = [col for col in required_cols if col in customers_df.columns]

                if available_cols:
                    customers_df = customers_df[available_cols]
                    # Add missing columns as None
                    for col in required_cols:
                        if col not in customers_df.columns:
                            customers_df[col] = None

                    # Access doesn't have ContactTitle, add default
                    customers_df['ContactTitle'] = 'Unknown'

                    access_data['customers_access'] = customers_df
                    print(f"  ✅ Access Customers transformed: {len(customers_df)} rows")
                else:
                    print("  ❌ Required columns not found in Customers")
                    access_data['customers_access'] = pd.DataFrame()

            except Exception as e:
                print(f"  ❌ Error extracting Access customers: {e}")
                access_data['customers_access'] = pd.DataFrame()

            #   5: Extract and transform Employees table
            try:
                print("  Extracting Employees...")
                employee_table = 'Employees'
                if employee_table not in table_list:
                    for table in table_list:
                        if 'employee' in table.lower():
                            employee_table = table
                            break

                query = f"SELECT * FROM [{employee_table}] WHERE [ID] IS NOT NULL"
                employees_df = pd.read_sql(query, access_conn)
                print(f"  ✅ Table {employee_table}: {len(employees_df)} rows")

                # Rename Access columns to standard names
                employees_df = employees_df.rename(columns={
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
                })

                # Add missing columns (Access doesn't have these)
                employees_df['TitleOfCourtesy'] = 'Mr.'  # Default value
                employees_df['ReportsTo'] = None  # Not in Access file
                employees_df['BirthDate'] = None  # Access might not have this
                employees_df['HireDate'] = None  # Access might not have this

                # Keep only necessary columns for DimEmployee
                required_cols = ['EmployeeID', 'LastName', 'FirstName', 'Title',
                                 'TitleOfCourtesy', 'BirthDate', 'HireDate', 'Address',
                                 'City', 'Region', 'PostalCode', 'Country', 'HomePhone', 'ReportsTo']
                available_cols = [col for col in required_cols if col in employees_df.columns]

                if available_cols:
                    employees_df = employees_df[available_cols]
                    # Add missing columns as None
                    for col in required_cols:
                        if col not in employees_df.columns:
                            employees_df[col] = None

                    access_data['employees_access'] = employees_df
                    print(f"  ✅ Access Employees transformed: {len(employees_df)} rows")
                else:
                    print("  ❌ Required columns not found in Employees")
                    access_data['employees_access'] = pd.DataFrame()

            except Exception as e:
                print(f"  ❌ Error extracting Access employees: {e}")
                access_data['employees_access'] = pd.DataFrame()

            #   6: Extract and transform Orders table
            try:
                print("  Extracting Orders...")
                orders_table = 'Orders'
                if orders_table not in table_list:
                    for table in table_list:
                        if 'order' in table.lower() and 'detail' not in table.lower():
                            orders_table = table
                            break

                #   6a: Inspect Orders table columns (Access can have different names)
                cursor = access_conn.cursor()
                cursor.execute(f"SELECT TOP 1 * FROM [{orders_table}]")
                columns = [column[0] for column in cursor.description]
                cursor.close()

                print(f"    Available columns in {orders_table}: {columns}")

                #   6b: Create intelligent column mapping
                # Map expected column names to possible Access column names
                expected_columns = {
                    'OrderID': ['Order ID', 'ID', 'OrderID'],
                    'CustomerID': ['Customer', 'Customer ID', 'CustomerID'],
                    'EmployeeID': ['Employee', 'Employee ID', 'EmployeeID'],
                    'OrderDate': ['Order Date', 'OrderDate'],
                    'RequiredDate': ['Required Date', 'RequiredDate'],
                    'ShippedDate': ['Shipped Date', 'ShippedDate'],
                    'ShipVia': ['Ship Via', 'ShipVia'],
                    'Freight': ['Shipping Fee', 'Freight', 'Ship Fee'],
                    'ShipName': ['Ship Name', 'ShipName'],
                    'ShipAddress': ['Ship Address', 'ShipAddress'],
                    'ShipCity': ['Ship City', 'ShipCity'],
                    'ShipRegion': ['Ship State/Province', 'Ship Region', 'State/Province'],
                    'ShipPostalCode': ['Ship ZIP/Postal Code', 'Ship Postal Code', 'ZIP/Postal Code'],
                    'ShipCountry': ['Ship Country/Region', 'Ship Country', 'Country/Region']
                }

                #   6c: Build dynamic SELECT query based on available columns
                select_columns = []
                available_columns = {}
                for expected_name, possible_names in expected_columns.items():
                    found = False
                    for possible_name in possible_names:
                        if possible_name in columns:
                            select_columns.append(f"[{possible_name}] as {expected_name}")
                            available_columns[expected_name] = possible_name
                            found = True
                            break
                    if not found:
                        print(f"    ⚠️  Column {expected_name} not found among {possible_names}")

                if not select_columns:
                    print("  ❌ No valid columns found for orders")
                    access_data['orders_access'] = pd.DataFrame()
                else:
                    # Build and execute query
                    select_clause = ", ".join(select_columns)
                    query = f"SELECT {select_clause} FROM [{orders_table}] WHERE [{available_columns.get('OrderID', 'ID')}] IS NOT NULL"

                    print(f"    Generated query: {query}")
                    orders_df = pd.read_sql(query, access_conn)
                    print(f"  ✅ Table {orders_table}: {len(orders_df)} rows")

                #   6d: Extract Order Details to calculate TotalAmount
                try:
                    print("  Extracting Order Details...")
                    order_details_table = 'Order Details'
                    if order_details_table not in table_list:
                        for table in table_list:
                            if 'order detail' in table.lower() or 'order_details' in table.lower():
                                order_details_table = table
                                break

                    details_query = f"""
                        SELECT od.[Order ID] as OrderID,
                               od.[Quantity],
                               od.[Unit Price] as UnitPrice,
                               od.[Discount]
                        FROM [{order_details_table}] od
                        WHERE od.[Order ID] IS NOT NULL
                    """
                    order_details_df = pd.read_sql(details_query, access_conn)
                    print(f"  ✅ Table {order_details_table}: {len(order_details_df)} rows")

                    if not order_details_df.empty:
                        # Calculate line totals and sum per order
                        order_details_df['LineTotal'] = order_details_df['Quantity'] * order_details_df['UnitPrice'] * (
                                    1 - order_details_df['Discount'])
                        totals = order_details_df.groupby('OrderID')['LineTotal'].sum().reset_index()
                        totals = totals.rename(columns={'LineTotal': 'TotalAmount'})

                        # Merge totals with orders
                        orders_df = pd.merge(orders_df, totals, on='OrderID', how='left')
                        orders_df['TotalAmount'] = orders_df['TotalAmount'].fillna(0)

                        print(f"  ✅ TotalAmount calculated for {len(orders_df)} orders")
                    else:
                        print("  ⚠️  No order details found")
                        orders_df['TotalAmount'] = 0

                except Exception as e:
                    print(f"  ⚠️  Cannot extract order details: {e}")
                    orders_df['TotalAmount'] = 0

                # Note: Access CustomerID/EmployeeID might be names, not IDs
                # This will need mapping later during loading
                access_data['orders_access'] = orders_df
                print(f"  ✅ Access Orders transformed: {len(orders_df)} rows")

            except Exception as e:
                print(f"  ❌ Error extracting Access orders: {e}")
                access_data['orders_access'] = pd.DataFrame()

            #   7: Close connection
            access_conn.close()

            #   8: Check if we got any data
            has_data = False
            for key, df in access_data.items():
                if not df.empty:
                    has_data = True
                    break

            if not has_data:
                print("  ℹ️  No data extracted from Access")

            return access_data

        except Exception as e:
            print(f"  ❌ Cannot access Access database: {e}")
            return {}





    #TRANSFORM / CLEAN FUNCTIONS
    # renaming
    # missing col
    # cleaning
    def transform_dim_customer(self, customers_df, source_name='SQL'):
        print(f"\n👥 TRANSFORMATION DIMCUSTOMER ({source_name})")
        print("-" * 30)

        if customers_df.empty:
            print("  ⚠️  no data client")
            return pd.DataFrame()

        dim_customer = customers_df.copy()

        # Mapping col based on the source
        if source_name == 'Access':
            column_mapping = {
                'ID': 'CustomerID',
                'Company': 'CompanyName',
                'First Name': 'FirstName',
                'Last Name': 'LastName',
                'Business Phone': 'Phone',
                'Address': 'Address',
                'City': 'City',
                'State/Province': 'Region',
                'ZIP/Postal Code': 'PostalCode',
                'Country/Region': 'Country'
            }
            # Combine FirstName + LastName into ContactName
            if 'FirstName' in dim_customer.columns and 'LastName' in dim_customer.columns:
                dim_customer['ContactName'] = dim_customer['FirstName'] + ' ' + dim_customer['LastName']
                dim_customer = dim_customer.drop(['FirstName', 'LastName'], axis=1, errors='ignore')

        else:  # SQL
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

        # Rename col
        for old_col, new_col in column_mapping.items():
            if old_col in dim_customer.columns and new_col not in dim_customer.columns:
                dim_customer = dim_customer.rename(columns={old_col: new_col})

        required_cols = list(column_mapping.values())
        available_cols = [col for col in required_cols if col in dim_customer.columns]

        if not available_cols:
            print("  ❌ no valid col found")
            return pd.DataFrame()

        dim_customer = dim_customer[available_cols]


        # missing col
        for col in required_cols:
            if col not in dim_customer.columns:
                dim_customer[col] = None

        dim_customer['SourceSystem'] = source_name

        # spicific Access cleaning
        if source_name == 'Access':
            #convert id to str
            if 'CustomerID' in dim_customer.columns:
                dim_customer['CustomerID'] = dim_customer['CustomerID'].astype(str)

            #id of clients -> acc-1..
            dim_customer['CustomerID'] = 'ACC-' + dim_customer['CustomerID'].astype(str)

        # general cleaning
        #Removes rows with null CustomerID
        if 'CustomerID' in dim_customer.columns:
            initial_count = len(dim_customer)
            dim_customer = dim_customer[dim_customer['CustomerID'].notna()]
            filtered_count = len(dim_customer)
            if filtered_count < initial_count:
                print(f"  ⚠️  {initial_count - filtered_count} CustomerID NULL ")


        #Fill nulls with 'Unknown'
        if 'Region' in dim_customer.columns:
            dim_customer['Region'] = dim_customer['Region'].fillna('Unknown')
        if 'PostalCode' in dim_customer.columns:
            dim_customer['PostalCode'] = dim_customer['PostalCode'].fillna('Unknown')
        if 'ContactTitle' in dim_customer.columns:
            dim_customer['ContactTitle'] = dim_customer['ContactTitle'].fillna('Unknown')

        #Convert all text to strings
        for col in dim_customer.columns:
            if dim_customer[col].dtype == 'object':
                dim_customer[col] = dim_customer[col].astype(str)

        print(f"  ✅ {len(dim_customer)} clients transformés")
        return dim_customer

    def transform_dim_employee(self, employees_df, source_name='SQL'):
        print(f"\n👨‍💼 TRANSFORMATION DIMEMPLOYEE ({source_name})")
        print("-" * 30)

        if employees_df.empty:
            print("  ⚠️  No employee data")
            return pd.DataFrame()

        dim_employee = employees_df.copy()

        # Mapping columns based on the source
        if source_name == 'Access':
            # LEFT SIDE: Actual Access column names from your Access file
            # RIGHT SIDE: Standardized column names for DimEmployee table
            column_mapping = {
                'ID': 'EmployeeID',  # Access has 'ID', rename to 'EmployeeID'
                'Last Name': 'LastName',  # Access has 'Last Name'
                'First Name': 'FirstName',  # Access has 'First Name'
                'Job Title': 'Title',  # Access has 'Job Title'
                'Business Phone': 'HomePhone',  # Access has 'Business Phone'
                'Address': 'Address',
                'City': 'City',
                'State/Province': 'Region',  # Access has 'State/Province'
                'ZIP/Postal Code': 'PostalCode',  # Access has 'ZIP/Postal Code'
                'Country/Region': 'Country'  # Access has 'Country/Region'
            }

            # Access doesn't have these columns, add defaults
            if 'TitleOfCourtesy' not in dim_employee.columns:
                # Add default courtesy title based on first name or generic
                dim_employee['TitleOfCourtesy'] = 'Mr.'  # Default value
            if 'BirthDate' not in dim_employee.columns:
                dim_employee['BirthDate'] = None  # Access might not have birth dates
            if 'HireDate' not in dim_employee.columns:
                dim_employee['HireDate'] = None  # Access might not have hire dates
            if 'ReportsTo' not in dim_employee.columns:
                dim_employee['ReportsTo'] = None  # Access might not have manager info

        else:  # SQL Server
            # SQL already has standardized column names
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

        # Define required columns for DimEmployee
        required_cols = [
            'EmployeeID', 'LastName', 'FirstName', 'Title', 'TitleOfCourtesy',
            'BirthDate', 'HireDate', 'Address', 'City', 'Region', 'PostalCode',
            'Country', 'HomePhone', 'ReportsTo'
        ]

        # Keep only available required columns
        available_cols = [col for col in required_cols if col in dim_employee.columns]

        if not available_cols:
            print("  ❌ No valid columns found")
            return pd.DataFrame()

        dim_employee = dim_employee[available_cols]

        # Add missing columns as None
        for col in required_cols:
            if col not in dim_employee.columns:
                dim_employee[col] = None

        # Tag each row with source system
        dim_employee['SourceSystem'] = source_name

        # Specific Access cleaning
        if source_name == 'Access':
            # Convert ID to numeric and prefix to avoid conflicts with SQL
            if 'EmployeeID' in dim_employee.columns:
                dim_employee['EmployeeID'] = pd.to_numeric(dim_employee['EmployeeID'], errors='coerce')
                # Prefix with 1000+ to avoid conflicts (e.g., ACC-1 becomes 1001)
                dim_employee['EmployeeID'] = 1000 + dim_employee['EmployeeID'].fillna(0).astype(int)

        # General cleaning

        # Remove rows with null EmployeeID
        if 'EmployeeID' in dim_employee.columns:
            initial_count = len(dim_employee)
            dim_employee = dim_employee[dim_employee['EmployeeID'].notna()]
            filtered_count = len(dim_employee)
            if filtered_count < initial_count:
                print(f"  ⚠️  {initial_count - filtered_count} rows filtered (EmployeeID NULL)")

        # Convert date columns to datetime
        date_cols = ['BirthDate', 'HireDate']
        for col in date_cols:
            if col in dim_employee.columns:
                dim_employee[col] = pd.to_datetime(dim_employee[col], errors='coerce')

        # Fill nulls with 'Unknown' for text columns
        if 'Region' in dim_employee.columns:
            dim_employee['Region'] = dim_employee['Region'].fillna('Unknown')
        if 'PostalCode' in dim_employee.columns:
            dim_employee['PostalCode'] = dim_employee['PostalCode'].fillna('Unknown')
        if 'Title' in dim_employee.columns:
            dim_employee['Title'] = dim_employee['Title'].fillna('Unknown')
        if 'TitleOfCourtesy' in dim_employee.columns:
            dim_employee['TitleOfCourtesy'] = dim_employee['TitleOfCourtesy'].fillna('Unknown')

        # Ensure numeric columns are properly typed
        if 'EmployeeID' in dim_employee.columns:
            dim_employee['EmployeeID'] = pd.to_numeric(dim_employee['EmployeeID'], errors='coerce')
            # Remove any remaining null EmployeeIDs
            dim_employee = dim_employee[dim_employee['EmployeeID'].notna()]

        if 'ReportsTo' in dim_employee.columns:
            dim_employee['ReportsTo'] = pd.to_numeric(dim_employee['ReportsTo'], errors='coerce')

        # Convert all text columns to strings
        for col in dim_employee.columns:
            if dim_employee[col].dtype == 'object':
                dim_employee[col] = dim_employee[col].astype(str)

        print(f"  ✅ {len(dim_employee)} employees transformed")
        return dim_employee

    def transform_fact_orders(self, orders_df, source_name='SQL'):
        print(f"\n📦 TRANSFORMATION FACTORDERS ({source_name})")
        print("-" * 30)

        if orders_df.empty:
            print("  ⚠️  No order data")
            return pd.DataFrame()

        fact_orders = orders_df.copy()

        # Mapping columns based on the source
        if source_name == 'Access':
            column_mapping = {
                'Order ID': 'OrderID',  # Access has 'Order ID'
                'Customer': 'CustomerID',  # Access has 'Customer' (might be name, not ID)
                'Employee': 'EmployeeID',  # Access has 'Employee' (might be name, not ID)
                'Order Date': 'OrderDate',  # Access has 'Order Date'
                'Required Date': 'RequiredDate',  # Access has 'Required Date'
                'Shipped Date': 'ShippedDate',  # Access has 'Shipped Date'
                'Shipping Fee': 'Freight',  # Access might have 'Shipping Fee' instead of 'Freight'
                'Ship Name': 'ShipName',  # Access has 'Ship Name'
                'Ship Address': 'ShipAddress',  # Access has 'Ship Address'
                'Ship City': 'ShipCity',  # Access has 'Ship City'
                'Ship State/Province': 'ShipRegion',  # Access has 'Ship State/Province'
                'Ship ZIP/Postal Code': 'ShipPostalCode',  # Access has 'Ship ZIP/Postal Code'
                'Ship Country/Region': 'ShipCountry'  # Access has 'Ship Country/Region'
            }

            # Note: Access might not have ShipVia, TotalAmount might need calculation
            if 'ShipVia' not in fact_orders.columns:
                fact_orders['ShipVia'] = 1  # Default shipping method
            if 'TotalAmount' not in fact_orders.columns:
                # If Access doesn't have TotalAmount, we'll need to calculate it later
                fact_orders['TotalAmount'] = 0.0

        else:  # SQL Server
            # SQL already has standardized column names
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

        # Define required columns for FactOrders
        required_cols = [
            'OrderID', 'CustomerID', 'EmployeeID', 'OrderDate',
            'RequiredDate', 'ShippedDate', 'ShipVia', 'Freight',
            'ShipName', 'ShipAddress', 'ShipCity', 'ShipRegion',
            'ShipPostalCode', 'ShipCountry', 'TotalAmount'
        ]

        # Keep only available required columns
        available_cols = [col for col in required_cols if col in fact_orders.columns]

        if not available_cols:
            print("  ❌ No valid columns found")
            return pd.DataFrame()

        fact_orders = fact_orders[available_cols]

        # Add missing columns as None
        for col in required_cols:
            if col not in fact_orders.columns:
                fact_orders[col] = None

        # Convert date columns to datetime
        date_cols = ['OrderDate', 'RequiredDate', 'ShippedDate']
        for col in date_cols:
            if col in fact_orders.columns:
                fact_orders[col] = pd.to_datetime(fact_orders[col], errors='coerce')

        # Calculate delivery status
        fact_orders['IsDelivered'] = fact_orders['ShippedDate'].notna().astype(int)

        # Calculate delivery delay (if shipped after required date)
        if 'ShippedDate' in fact_orders.columns and 'RequiredDate' in fact_orders.columns:
            fact_orders['DeliveryDelayDays'] = np.where(
                fact_orders['ShippedDate'].notna() & fact_orders['RequiredDate'].notna(),
                (fact_orders['ShippedDate'] - fact_orders['RequiredDate']).dt.days,
                None
            )
        else:
            fact_orders['DeliveryDelayDays'] = None

        # Tag each row with source system
        fact_orders['SourceSystem'] = source_name

        # Specific Access processing
        if source_name == 'Access':
            # Access CustomerID/EmployeeID might be names, not IDs
            # We'll need to map them later during loading
            if 'CustomerID' in fact_orders.columns:
                # Check if it's numeric (ID) or text (Company Name)
                # Convert numeric IDs to string
                try:
                    fact_orders['CustomerID'] = pd.to_numeric(fact_orders['CustomerID'], errors='coerce')
                    # If numeric, prefix with ACC-
                    fact_orders['CustomerID'] = 'ACC-' + fact_orders['CustomerID'].astype(str)
                except:
                    # If not numeric, it's probably a company name
                    # We'll need to map it later
                    pass

            if 'EmployeeID' in fact_orders.columns:
                # Check if it's numeric (ID) or text (Employee Name)
                try:
                    fact_orders['EmployeeID'] = pd.to_numeric(fact_orders['EmployeeID'], errors='coerce')
                    # If numeric, add 1000 like in employee transformation
                    fact_orders['EmployeeID'] = 1000 + fact_orders['EmployeeID'].fillna(0).astype(int)
                except:
                    # If not numeric, it's probably an employee name
                    # We'll need to map it later
                    pass

        # Ensure numeric columns are properly typed
        if 'Freight' in fact_orders.columns:
            fact_orders['Freight'] = pd.to_numeric(fact_orders['Freight'], errors='coerce').fillna(0)
        if 'TotalAmount' in fact_orders.columns:
            fact_orders['TotalAmount'] = pd.to_numeric(fact_orders['TotalAmount'], errors='coerce').fillna(0)
        if 'ShipVia' in fact_orders.columns:
            fact_orders['ShipVia'] = pd.to_numeric(fact_orders['ShipVia'], errors='coerce').fillna(1)

        # Convert all text columns to strings
        for col in fact_orders.columns:
            if fact_orders[col].dtype == 'object':
                fact_orders[col] = fact_orders[col].astype(str)

        print(f"  ✅ {len(fact_orders)} orders transformed")
        return fact_orders













    def run_full_etl(self):
        print("\n" + "=" * 50)
        print("🚀 FULL ETL ")
        print("=" * 50)

        try:
            # create / fill DimDate
            self.fill_dim_date(1990, 2025)

            # extract from sql server
            sql_data = self.extract_from_sql_server()

            # Transform SQL data
            dim_customer_sql = self.transform_dim_customer(sql_data.get('customers', pd.DataFrame()), 'SQL')
            dim_employee_sql = self.transform_dim_employee(sql_data.get('employees', pd.DataFrame()), 'SQL')
            fact_orders_sql = self.transform_fact_orders(sql_data.get('orders', pd.DataFrame()), 'SQL')


            # extract + transform from access
            access_data = self.extract_from_access()

            if access_data:
                dim_customer = pd.concat([
                    dim_customer_sql,
                    access_data.get('customers_access', pd.DataFrame())
                ], ignore_index=True)

                dim_employee = pd.concat([
                    dim_employee_sql,
                    access_data.get('employees_access', pd.DataFrame())
                ], ignore_index=True)

                fact_orders = pd.concat([
                    fact_orders_sql,
                    access_data.get('orders_access', pd.DataFrame())
                ], ignore_index=True)
            else:
                dim_customer = dim_customer_sql
                dim_employee = dim_employee_sql
                fact_orders = fact_orders_sql




            # Étape 5: Charger les dimensions
            self.load_dimensions_to_dw(dim_customer, dim_employee)

            # Étape 6: Charger les faits (AJOUTÉ)
            self.load_facts_to_dw(fact_orders)

            # Étape 7: Sauvegarder pour dashboard
            print("\n🎯 PRÉPARATION POUR DASHBOARD")
            print("-" * 30)

            try:
                import os
                if not os.path.exists('data'):
                    os.makedirs('data')
                fact_orders.to_csv('data/fact_orders_transformed.csv', index=False)
                print("  ✅ Données sauvegardées dans data/fact_orders_transformed.csv")
            except Exception as e:
                print(f"  ⚠️  Impossible de sauvegarder les données: {e}")

            self.show_summary()

            print("\n" + "=" * 50)
            print("🎉 PROCESSUS ETL TERMINÉ AVEC SUCCÈS!")
            print("=" * 50)

        except Exception as e:
            print(f"\n❌ ERREUR CRITIQUE DANS L'ETL: {e}")
            raise
