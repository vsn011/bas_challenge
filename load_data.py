import pandas as pd
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine
import dotenv
import os
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('_name_')
dotenv.load_dotenv()


engine = create_engine(
    URL(
        drivername="postgresql",
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        database="postgres",
    )
)
conn = engine.connect()

sheet_list = ['Customers', 'SLAs', 'Customer2SLA', 'TimeBooking', 'Employee']
tables = {s: pd.read_excel(os.path.join("data", "BASCaseStudy.xlsx"), s, engine='openpyxl') for s in sheet_list}

for table_name, table in tables.items():
    table_df = pd.DataFrame(table)
    table_df.to_sql(table_name, engine, schema='homework', index=False, if_exists='replace', method='multi')
