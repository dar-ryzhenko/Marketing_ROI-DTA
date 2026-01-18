import numpy as np
import pandas as pd
import db_sql as db
import os
import matplotlib.pyplot as plt
import seaborn as sns

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

  # Витрати по каналах і перевірка повноти
  def check_channel_completeness(marketing_clean: pd.DataFrame) -> pd.DataFrame:
      pivot = marketing_clean.pivot_table(
          index="month", columns="channel", values="spend_amount", aggfunc="sum"
      )
      # Доповнимо очікуваними каналами, навіть якщо їх немає в даних
      expected_channels = ["Facebook", "Google Ads", "Instagram", "TikTok", "YouTube"]
      for ch in expected_channels:
          if ch not in pivot.columns:
              pivot[ch] = np.nan
      pivot = pivot[expected_channels]  # впорядкуємо колонки

      # Додамо прапорці повноти
      pivot["complete_all_channels"] = pivot[expected_channels].notna().all(axis=1)
      pivot["missing_channels"] = pivot[expected_channels].isna().sum(axis=1)
      return pivot.reset_index()

  channel_check = check_channel_completeness(df_marketing_clean)

  # Графік: Продажі vs Витрати
  def plot_sales_vs_spend(sales_marketing: pd.DataFrame, marketing_clean: pd.DataFrame):
      # Лінія продажів
      fig, ax = plt.subplots()

      ax.plot(sales_marketing["month"], sales_marketing["total_sales"], color="black", label="Продажі")
      ax.set_xlabel("Місяць")
      ax.set_ylabel("Сума продажів")
      ax.legend(loc="upper left")

      plt.title("Продажі (лінія) та маркетингові витрати по каналах (стек)")

      # Підготуємо стек витрат по каналах
      pivot = marketing_clean.pivot_table(
          index="month", columns="channel", values="spend_amount", aggfunc="sum"
      ).fillna(0)
      # Впорядкуємо канали
      cols = ["Facebook", "Google Ads", "Instagram", "TikTok", "YouTube"]
      for c in cols:
          if c not in pivot.columns:
              pivot[c] = 0.0
      pivot = pivot[cols].sort_index()

      # Друга вісь для витрат
      ax2 = ax.twinx()
      ax2.stackplot(
          pivot.index,
          [pivot[c].values for c in cols],
          labels=cols,
          alpha=0.4
      )
      ax2.set_ylabel("Маркетингові витрати")
      ax2.legend(loc="upper right")
      plt.tight_layout()
      plt.show()

  # plot_sales_vs_spend(sales_marketing, df_marketing_clean)    графік

  # Таблиці для презентації топ-3 клієнтів
  def build_top3_customers(orders_df: pd.DataFrame) -> pd.DataFrame:
    top3 = (
        orders_df.groupby("customer_id", as_index=False)
        .agg(orders_count=("order_id", "count"), total_spent=("order_amount", "sum"))
        .sort_values("total_spent", ascending=False)
        .head(3)
    )
    return top3

  top3_customers = build_top3_customers(orders)

  top3_customers.to_csv("out_top3_customers.csv", index=False)
  channel_check.to_csv("out_channel_check.csv", index=False)




if __name__ == "__main__":
  main()