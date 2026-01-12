import numpy as np
import pandas as pd
import db_sql as db
import os

from sqlalchemy import text

from dotenv import load_dotenv
load_dotenv()

monthly_category_sql = """
SELECT to_char(order_date, 'YYYY-MM') AS month,
       product_category, 
       COUNT(order_id) AS order_count,
       SUM(order_amount) AS total_sales
  
FROM orders
GROUP BY 1, 2; -- month, product_category
"""

monthly_sql = """
SELECT DATE_TRUNC('month', order_date) AS month,
  COUNT(order_id) AS order_count,
  SUM(order_amount) AS total_sales
FROM orders
GROUP BY month;
"""

top3_customers_sql = """
SELECT customer_id, 
  SUM(order_amount) AS sum_amount
FROM orders
GROUP BY customer_id
ORDER BY sum_amount desc
LIMIT 3;
"""


def load_marketing_csv(csv_path: str)-> pd.DataFrame:
  df = pd.read_csv(csv_path)
  # month	channel	spend_amount
  df["month"] = pd.to_datetime(df["month"], errors="coerce") 
  return df

def clean_marketing_data(df_raw: pd.DataFrame) -> pd.DataFrame:
  df = df_raw.copy()
  df["channel"] = df["channel"].astype(str).str.strip()
  # Facebook, Google Ads, Instagram, TikTok, YouTube
  channel_map = {
    "google ads": "Google Ads",
    "google ad": "Google Ads",
    "googleads": "Google Ads",
    "facebook": "Facebook",
    "instagram": "Instagram",
    "tiktok": "TikTok",
    "youtube": "YouTube"
    }
  df['channel_std'] = df["channel"].str.lower().map(channel_map).fillna(df["channel"].str.title())

  def parse_spend(x):
    if pd.isna(x):
      return np.nan
    x = str(x).strip().lower()
    if x in ["", "na", "missing", "null", "none"]:
      return np.nan
    # if not str(x).isdigit():
    #     return np.nan  

    try:
      return float(x)
    except:
      return np.nan
  
  df["spend_amount_num"] = df["spend_amount"].apply(parse_spend)
  df["negative_spend_flag"] = df["spend_amount_num"] < 0
  df.loc[df["negative_spend_flag"], "spend_amount_num"] = np.nan
  clean = df[["month", "channel_std", "spend_amount_num", "negative_spend_flag"]].rename(columns={"channel_std": "channel", "spend_amount_num":"spend_amount"})

  clean["month"] = pd.to_datetime(clean["month"]).dt.to_period("M").dt.to_timestamp()
  return clean



def main():
  csv_path = "marketing_spend.csv"
  df_marketing_clean = clean_marketing_data(load_marketing_csv(csv_path))
  print(df_marketing_clean)

  orders_sql_path = "orders.sql"
  pg_url = os.getenv("POSTGRES_URL")
  pg_engine = db.get_postgres_engine(pg_url)

  try:
    with open(orders_sql_path, "r", encoding="utf-8") as f:
      orders_sql = f.read()
    # print(orders_sql)

    with pg_engine.begin() as conn:
      for s in orders_sql.split(";"):
        s = s.strip()
        if s: 
          conn.execute(text(orders_sql))

    #pd.read_sql(orders_sql, con=pg_engine) #створення таблиці orders
  except Exception as e:
    print(e)

  orders = db.load_orders_postgres(pg_engine)

  print(orders)


  def agg_sales_monthly(orders_df: pd.DataFrame)-> pd.DataFrame:
    df = orders_df.copy()
    df["month"] = df["order_date"].dt.to_period("M").dt.to_timestamp()
    monthly_sales=df.groupby('month', as_index=False).agg(
      order_count = ("order_id", "count"),#COUNT(order_id)
      total_sales = ("order_amount", "sum")#SUM(order_amount)
    )
    return monthly_sales
  monthly_sales = agg_sales_monthly(orders)
  #print(monthly_sales)
  monthly_sales.to_csv("monthly_sales.csv", index=False)

  def merge_sales_marketing(df_marketing_clean: pd.DataFrame, monthly_sales: pd.DataFrame) -> pd.DataFrame:
    df_marketing_clean= (
      df_marketing_clean.groupby('month', as_index= False).agg(marketing_spend = ("spend_amount", "sum"))
      )

    merged = monthly_sales.merge(df_marketing_clean, on="month", how="left")
    return merged
  sales_marketing = merge_sales_marketing(df_marketing_clean, monthly_sales)
  #print(sales_marketing)
  sales_marketing.to_csv("sales_marketing.csv", index=False)

  #ROI
  def monthly_roi(sales_marketing_df: pd.DataFrame)->pd.DataFrame:
    df = sales_marketing.copy()
    df["roi"] = np.where(
      df["marketing_spend"] > 0,
      df["total_sales"] / df["marketing_spend"],
      np.nan
    )
    return df[["month", "total_sales", "marketing_spend", "roi"]]
  monthly_roi = monthly_roi(sales_marketing)
  monthly_roi.to_csv("monthly_roi.csv", index=False)

if __name__ == "__main__":
  main()