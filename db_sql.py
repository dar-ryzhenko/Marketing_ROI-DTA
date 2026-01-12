from sqlalchemy import create_engine
import pandas as pd

def get_postgres_engine(pg_url: str):
    engine = create_engine(pg_url, echo=False, future= True)
    return engine

def load_orders_postgres(engine):
    query = '''
            SELECT *
            FROM orders;
            -- LIMIT 5;
            '''
    df = pd.read_sql(query, con=engine)
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["order_amount"] = pd.to_numeric(df["order_amount"], errors="coerce")
    return df

