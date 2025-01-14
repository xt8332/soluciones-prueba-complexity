from fastapi import FastAPI, Query
from sqlalchemy import create_engine
import pandas as pd

app = FastAPI()
db_url = "sqlite:///sales_dashboard.db"
engine = create_engine(db_url)

@app.get("/sales/product")
def get_product_sales(product: str = None, category: str = None):
    query = "SELECT product, SUM(total_sales) as total_sales FROM transactions"
    conditions = []
    if product:
        conditions.append(f"product = '{product}'")
    if category:
        conditions.append(f"category = '{category}'")
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " GROUP BY product"
    return pd.read_sql(query, engine).to_dict(orient="records")

@app.get("/sales/day")
def get_day_sales(start_date: str = None, end_date: str = None):
    query = "SELECT date, SUM(total_sales) as total_sales FROM transactions"
    if start_date and end_date:
        query += f" WHERE date BETWEEN '{start_date}' AND '{end_date}'"
    query += " GROUP BY date"
    return pd.read_sql(query, engine).to_dict(orient="records")

@app.get("/sales/category")
def get_category_metrics():
    query = "SELECT * FROM aggregated_metrics"
    return pd.read_sql(query, engine).to_dict(orient="records")

@app.get("/sales/outliers")
def get_outliers():
    query = "SELECT * FROM outliers"
    return pd.read_sql(query, engine).to_dict(orient="records")
