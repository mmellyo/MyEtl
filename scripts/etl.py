import os.path

from sqlalchemy import create_engine
import pandas as pd

def extract() :

    # connect to sql
    engine_sql = create_engine(
        "mssql+pyodbc://localhost\\SQLEXPRESS/Northwind?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"
    )

    dfs_sql = {}  # dict to store data framme that contains needed sqlServer tables
    tables_sql = ["Customers", "Employees", "EmployeeTerritories", "Orders", "Region","Territories" ]
    cpt_sql = 0 #debug

    for table in tables_sql:
        try :
            dfs_sql[f"{table}"] = pd.read_sql(f"SELECT * FROM {table}", engine_sql)

            cpt_sql += 1 #debug
            print(f"Loaded {table}_sql successfully") #debug
        except:
            print(f"error {table}_sql ")
    print(cpt_sql) #debug



    #------------------------
    #access (read from excel)
    path = os.path.join(os.path.dirname(__file__), "..", "data", "raw")

    dfs_acc = {} # dict to store data frames that contains needed access tables
    tables_acc = ["Customers", "Employees","Order Details", "Order Details Status", "Orders", "Orders Status"]
    cpt_acc = 0 #debug

    for table in tables_acc:
        try:
            dfs_acc[f"{table}"] = pd.read_excel(os.path.join(path, f"{table}.xlsx"))
            cpt_acc +=1 #debug
            print(f"Loaded {table}_acc successfully")  #debug


        except:
            print(f"error {table}_acc ")
    print(cpt_acc) #debug

    print("EXPORT DONE")
    return dfs_sql, dfs_acc





def transform(dfs_sql, dfs_acc):
    rename_maps = {
        "Customers": {
            "ID": "CustomerID",
            "Last Name": "LastName",
            "First Name": "FirstName",
        },
        "Employees": {
            "ID": "EmployeeID",
            "Last Name": "LastName",
            "First Name": "FirstName",
        },
        "Orders": {
            "Order ID": "OrderID",
            "Order Date": "OrderDate",
            "Shipped Date": "ShippedDate",
        }
    }

    for table_name, df_acc in dfs_acc.items():
        # remove spaces from col & table names
        df_acc.columns = [col.replace(" ", "") for col in df_acc.columns]
        table_name = table_name.replace(" ", "")

        # rename columns
        if table_name in rename_maps:
            df_acc.rename(columns=rename_maps[table_name], inplace=True)
            print(f"Renamed columns in table: {table_name}")

        print(f"table ==> {table_name}, its col ===> {df_acc.columns}")

    return dfs_sql, dfs_acc



if __name__ == '__main__':
    dfs_sql, dfs_acc = extract()
    transform(dfs_sql, dfs_acc)
