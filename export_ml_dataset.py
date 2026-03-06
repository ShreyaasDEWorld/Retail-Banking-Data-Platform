import pandas as pd
from db_connection import get_connection

conn = get_connection()

query = "SELECT * FROM analytics.ml_training_dataset"

df = pd.read_sql(query, conn)

df.to_csv("ml_training_dataset.csv", index=False)

conn.close()

print("Dataset exported successfully.")