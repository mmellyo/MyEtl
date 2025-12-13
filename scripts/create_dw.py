import pyodbc
import DatabaseConfig


def create_datawarehouse():
    try:
        conn = pyodbc.connect(
            f'DRIVER={{SQL Server}};'
            f'SERVER={DatabaseConfig.SQL_SERVER_INSTANCE};'
            f'DATABASE=master;'
            'Trusted_Connection=yes;'
        )
        cursor = conn.cursor()

        # Check if database exists
        cursor.execute(f"SELECT name FROM sys.databases WHERE name = '{DatabaseConfig.TARGET_DATABASE}'")
        if cursor.fetchone():
            print(f"Database '{DatabaseConfig.TARGET_DATABASE}' already exists")
            cursor.close()
            conn.close()
            return True

        # else create database
        cursor.execute(f"CREATE DATABASE {DatabaseConfig.TARGET_DATABASE}")
        conn.commit()
        print(f"Database '{DatabaseConfig.TARGET_DATABASE}' created successfully")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"Error creating database: {e}")
        return False


#create tables
def create_dw_schema():
    try:
        conn = pyodbc.connect(
            f'DRIVER={{SQL Server}};'
            f'SERVER={DatabaseConfig.SQL_SERVER_INSTANCE};'
            f'DATABASE={DatabaseConfig.TARGET_DATABASE};'
            'Trusted_Connection=yes;'
        )
        cursor = conn.cursor()

        # DimDate
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='DimDate' AND xtype='U')
            CREATE TABLE DimDate (
                DateKey INT PRIMARY KEY,
                Date DATE NOT NULL,
                Year INT NOT NULL,
                Quarter INT NOT NULL,
                Month INT NOT NULL,
                Day INT NOT NULL,
                MonthName VARCHAR(20),
                DayOfWeek VARCHAR(20),
                IsWeekend BIT,
                UNIQUE(Date)
            )
        """)
        print("DimDate table created/verified")

        #DimCustomer
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='DimCustomer' AND xtype='U')
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
            )
        """)
        print("DimCustomer table created/verified")

        # DimEmployee
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='DimEmployee' AND xtype='U')
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
            )
        """)
        print("DimEmployee table created/verified")

        #  FactOrders
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='FactOrders' AND xtype='U')
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
                SourceSystem VARCHAR(20)
                -- Foreign keys will be added after tables exist
            )
        """)
        print("FactOrders table created/verified")

        conn.commit()
        cursor.close()
        conn.close()
        print("Data warehouse schema created successfully")
        return True

    except Exception as e:
        print(f"Error creating schema: {e}")
        return False


def add_foreign_keys():
    try:
        conn = pyodbc.connect(
            f'DRIVER={{SQL Server}};'
            f'SERVER={DatabaseConfig.SQL_SERVER_INSTANCE};'
            f'DATABASE={DatabaseConfig.TARGET_DATABASE};'
            'Trusted_Connection=yes;'
        )
        cursor = conn.cursor()

        # for CustomerKey
        try:
            cursor.execute("""
                IF NOT EXISTS (
                    SELECT * FROM sys.foreign_keys 
                    WHERE name = 'FK_FactOrders_DimCustomer'
                )
                ALTER TABLE FactOrders
                ADD CONSTRAINT FK_FactOrders_DimCustomer 
                FOREIGN KEY (CustomerKey) REFERENCES DimCustomer(CustomerKey)
            """)
            print("Foreign key FK_FactOrders_DimCustomer added")
        except:
            print("Foreign key FK_FactOrders_DimCustomer already exists or couldn't be added")

        # for EmployeeKey
        try:
            cursor.execute("""
                IF NOT EXISTS (
                    SELECT * FROM sys.foreign_keys 
                    WHERE name = 'FK_FactOrders_DimEmployee'
                )
                ALTER TABLE FactOrders
                ADD CONSTRAINT FK_FactOrders_DimEmployee 
                FOREIGN KEY (EmployeeKey) REFERENCES DimEmployee(EmployeeKey)
            """)
            print("Foreign key FK_FactOrders_DimEmployee added")
        except:
            print("Foreign key FK_FactOrders_DimEmployee already exists or couldn't be added")

        # for OrderDateKey
        try:
            cursor.execute("""
                IF NOT EXISTS (
                    SELECT * FROM sys.foreign_keys 
                    WHERE name = 'FK_FactOrders_DimDate'
                )
                ALTER TABLE FactOrders
                ADD CONSTRAINT FK_FactOrders_DimDate 
                FOREIGN KEY (OrderDateKey) REFERENCES DimDate(DateKey)
            """)
            print("Foreign key FK_FactOrders_DimDate added")
        except:
            print("Foreign key FK_FactOrders_DimDate already exists or couldn't be added")

        # Create indexes for performance
        try:
            cursor.execute("""
                IF NOT EXISTS (
                    SELECT * FROM sys.indexes 
                    WHERE name = 'IX_FactOrders_OrderDateKey'
                )
                CREATE INDEX IX_FactOrders_OrderDateKey ON FactOrders(OrderDateKey)
            """)
            print("Index IX_FactOrders_OrderDateKey created")
        except:
            print("Index IX_FactOrders_OrderDateKey already exists or couldn't be created")

        try:
            cursor.execute("""
                IF NOT EXISTS (
                    SELECT * FROM sys.indexes 
                    WHERE name = 'IX_FactOrders_CustomerKey'
                )
                CREATE INDEX IX_FactOrders_CustomerKey ON FactOrders(CustomerKey)
            """)
            print("Index IX_FactOrders_CustomerKey created")
        except:
            print("Index IX_FactOrders_CustomerKey already exists or couldn't be created")

        try:
            cursor.execute("""
                IF NOT EXISTS (
                    SELECT * FROM sys.indexes 
                    WHERE name = 'IX_FactOrders_EmployeeKey'
                )
                CREATE INDEX IX_FactOrders_EmployeeKey ON FactOrders(EmployeeKey)
            """)
            print("Index IX_FactOrders_EmployeeKey created")
        except:
            print("Index IX_FactOrders_EmployeeKey already exists or couldn't be created")

        conn.commit()
        cursor.close()
        conn.close()
        print("Foreign keys and indexes added successfully")
        return True

    except Exception as e:
        print(f"Error adding foreign keys: {e}")
        return False


if __name__ == "__main__":
    print("Creating data warehouse...")
    if create_datawarehouse():
        if create_dw_schema():
            add_foreign_keys()
            print("Data warehouse setup complete!")