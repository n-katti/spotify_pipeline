import sqlalchemy as sql
import dotenv
import os
import pandas as pd

dotenv.load_dotenv()

user = os.environ.get("sql_username")
pw = os.environ.get("sql_pw")
db = os.environ.get("sql_db")
endpoint = os.environ.get("sql_endpoint")

def create_engine():
    engine = sql.create_engine(f'postgresql://{user}:{pw}@{endpoint}/{db}')
    return engine

def run_query(query):
    conn = engine.connect()
    result = conn.execute(query)
    return result

engine = create_engine()
connection = engine.connect()

# query = '''
# CREATE TABLE test_table (name VARCHAR);

# INSERT INTO test_table VALUES ('nikhil');
# '''

query2 = '''
SELECT * FROM test_table'''

# run_query(query)

results = run_query(query2)

# for row in results:
#     print(row)

df = pd.read_sql(query2, connection)

print(df)