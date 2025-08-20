from sqlalchemy import create_engine
import pyodbc
import pandas as pd
import os

pwd=os.environ.get('PGPASS','demopass')
uid=os.environ.get('PGUID','etl')

driver="{SQL server}"
server="DESKTOP-3AU3UM5\\SQLLEARNER"
database="ADF" 

def extract():
    try:
        src_conn=pyodbc.connect('DRIVER='+ driver + ';SERVER=' + server +';DATABASE='+database+';UID='+uid+';PWD='+pwd)
        src_cursor=src_conn.cursor()
        src_cursor.execute("""select t.name as table_name from sys.tables t""")
        src_tables=src_cursor.fetchall()
        for tbl in src_tables:
            df=pd.read_sql_query(f'select * from {tbl[0]}',src_conn)
            load(df,tbl[0])
    except Exception as e:
        print("Data Extract Error" + str(e))
    finally:
        src_conn.close()

def load(df,tbl):
    try:
        rows_imported=0
        engine=create_engine(f'postgresql://{uid}:{pwd}@localhost:5432/ETL')
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        df.to_sql(f'{tbl}',engine,if_exists='replace',index=False)
        rows_imported+=len(df)
        print("Data imported successfully")
    except Exception as e:
        print("Data load error:"+str(e))

try:
    extract()
except Exception as e:
    print("Error while extracting data:"+ str(e))               
