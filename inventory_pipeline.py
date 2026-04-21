import pandas as pd
import mysql.connector
import json
import os
from datetime import datetime, timedelta

# ===== LOGGING =====
def log(msg):
    os.makedirs("logs", exist_ok=True)
    with open("logs/log.txt", "a") as f:
        f.write(f"{datetime.now()} - {msg}\n")


# ===== DB CONNECTION =====
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Root@123"
)
cursor = conn.cursor()

cursor.execute("CREATE DATABASE IF NOT EXISTS inventory_pipeline")
cursor.execute("USE inventory_pipeline")


# ===== CREATE TABLE =====
def create_table():
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS inventory (
        sku VARCHAR(50),
        stock INT,
        daily_sales FLOAT,
        days_left FLOAT,
        stockout_date DATE,
        restock_qty INT,
        urgency VARCHAR(20),
        priority_score FLOAT
    )
    """)
    conn.commit()
    log("Table created / already exists")


# ===== EXTRACT =====
def extract_data():
    try:
        stock = pd.read_csv("data/Warehouse_Stock.csv")
        sales = pd.read_csv("data/Sales_Velocity.csv")
        log("CSV files loaded successfully")
        return stock, sales
    except Exception as e:
        log(f"Error reading CSV: {e}")
        return None, None


# ===== TRANSFORM =====
def process_data(stock, sales):
    try:
        df = pd.merge(stock, sales, on="sku")

        # Validation
        df = df[df["stock"] >= 0]
        df = df.dropna()

        # Safe days calculation
        df["days_left"] = df.apply(
            lambda row: row["stock"] / row["daily_sales"] if row["daily_sales"] != 0 else 0,
            axis=1
        )

        df["days_left"] = df["days_left"].replace([float("inf")], 0)

        # Stockout date
        df["stockout_date"] = df["days_left"].apply(
            lambda x: (datetime.now() + timedelta(days=x)).strftime("%Y-%m-%d")
        )

        # Restock logic
        df["restock_qty"] = (df["daily_sales"] * 5 + 20) - df["stock"]
        df["restock_qty"] = df["restock_qty"].apply(lambda x: max(0, int(x)))

        # Urgency
        def get_urgency(days):
            if days < 3:
                return "High"
            elif days < 7:
                return "Medium"
            else:
                return "Low"

        df["urgency"] = df["days_left"].apply(get_urgency)

        # Priority
        df["priority_score"] = df.apply(
            lambda row: (1 / row["days_left"]) * row["daily_sales"] if row["days_left"] != 0 else 0,
            axis=1
        )

        log("Data processed successfully")
        return df

    except Exception as e:
        log(f"Error in processing data: {e}")
        return None


# ===== LOAD =====
def load_data(df):
    try:
        query = """
        INSERT INTO inventory
        (sku, stock, daily_sales, days_left, stockout_date, restock_qty, urgency, priority_score)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        data = [
            (
                row["sku"],
                int(row["stock"]),
                float(row["daily_sales"]),
                float(row["days_left"]),
                row["stockout_date"],
                int(row["restock_qty"]),
                row["urgency"],
                float(row["priority_score"])
            )
            for _, row in df.iterrows()
        ]

        cursor.executemany(query, data)
        conn.commit()
        log("Data inserted into database")

    except Exception as e:
        log(f"Error inserting into DB: {e}")


# ===== OUTPUT =====
def generate_output(df):
    try:
        os.makedirs("output", exist_ok=True)

        result = df[[
            "sku", "restock_qty", "urgency",
            "stockout_date", "priority_score"
        ]].to_dict(orient="records")

        with open("output/Purchase_Order.json", "w") as f:
            json.dump(result, f, indent=4)

        log("JSON output generated")

    except Exception as e:
        log(f"Error generating JSON: {e}")


# ===== PIPELINE =====
def run_pipeline():
    log("Pipeline started")

    create_table()

    stock, sales = extract_data()

    if stock is None or sales is None:
        log("Stopping pipeline due to data load error")
        return

    df = process_data(stock, sales)

    if df is None:
        log("Stopping pipeline due to processing error")
        return

    load_data(df)

    generate_output(df)

    log("Pipeline completed")
    print("✅ Inventory Pipeline Completed")


# ===== START =====
if __name__ == "__main__":
    run_pipeline()