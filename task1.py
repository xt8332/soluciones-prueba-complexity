import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

# Carga de csv
file_path = "csv_complexity.csv" 
data = pd.read_csv(file_path)

# 1. Data Cleaning
# Se reemplaza los missing quantities con 0
data['quantity'].fillna(0, inplace=True)

# Se reemplaza los precios invalidos wiht el precio de la categoria
data['price'] = pd.to_numeric(data['price'], errors='coerce')  # Convertir a numérico, inválido a NaN
category_medians = data.groupby('category')['price'].transform('median')
data['price'].fillna(category_medians, inplace=True)

# Eliminar filas donde faltan o no son válidos tanto la cantidad como el precio
data.dropna(subset=['quantity', 'price'], how='all', inplace=True)

# 2. Derived Columns
# Se calcula total_sales
data['total_sales'] = data['quantity'] * data['price']

# Se agrega day_of_week
data['day_of_week'] = pd.to_datetime(data['date']).dt.day_name()

# High volume flag
data['high_volume'] = data['quantity'] > 10

# 3. Complex Transformations
# Group by category
category_metrics = data.groupby('category').agg(
    avg_price=('price', 'mean'),
    total_revenue=('total_sales', 'sum'),
    highest_sales_day=('date', lambda x: x.value_counts().idxmax())
).reset_index()

# Identificar valores atípicos (cantidad > 2 desviaciones estándar de la media de la categoría)
category_means = data.groupby('category')['quantity'].transform('mean')
category_stds = data.groupby('category')['quantity'].transform('std')
data['outlier'] = (data['quantity'] > category_means + 2 * category_stds)

# 4. Conección con SQLite
conn = sqlite3.connect("sales_dashboard.db")

# Store transactions
data.to_sql('transactions', conn, if_exists='replace', index=False)

# Store aggregated metrics
category_metrics.to_sql('aggregated_metrics', conn, if_exists='replace', index=False)

# Store outliers
outliers = data[data['outlier']]
outliers.to_sql('outliers', conn, if_exists='replace', index=False)

print("Procesamiento de datos completado y almacenado en SQLite.")
