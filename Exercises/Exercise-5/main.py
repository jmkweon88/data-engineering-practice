import psycopg2
import os
import glob
import sys

sqls = dict()

sqls["accounts"] = """CREATE TABLE IF NOT EXISTS accounts (
    customer_id INTEGER PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    address_1 VARCHAR,
    address_2 VARCHAR,
    city VARCHAR,
    state VARCHAR,
    zip_code INTEGER,
    join_date DATE
)"""

sqls["products"] = """CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY,
    product_code VARCHAR,
    product_description VARCHAR
)"""

sqls["transactions"] = """CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR PRIMARY KEY,
    transaction_date DATE, 
    product_id INTEGER REFERENCES products(product_id), 
    product_code VARCHAR, 
    product_description VARCHAR, 
    quantity INTEGER, 
    account_id INTEGER REFERENCES accounts(customer_id)
)"""

def drop(tablename):
    drop_sql = f"""DROP TABLE IF EXISTS {tablename}"""
    return drop_sql

def csv_copy(filepath, tablename):
    with open(filepath,'r') as c:
        header = c.readline().strip()
    copy_sql = f"""COPY {tablename}({header})
FROM '{filepath}'
DELIMITER ','
CSV HEADER"""
    return(copy_sql)



def main():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    # your code here
    fps = glob.glob('/data/*.csv',recursive=True)
    tns = [os.path.basename(i).split('.')[0] for i in fps]
    cur = conn.cursor()
    for i in range(len(fps)):
        fp = fps[i]
        tn = tns[i].replace('.csv','')
        cur.execute(drop(tn))
        cur.execute(sqls[tn])
        cur.execute(csv_copy(fp, tn))
        cur.copy_to(sys.stdout, i, sep = ",")

if __name__ == "__main__":
    main()