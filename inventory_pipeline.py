import pandas as pd
import mysql.connector
import json
import os
from datetime import datetime, timedelta

# ================= LOGGING =================
def log(msg):
    os.makedirs("logs", exist_ok=True)
    with open("logs/log.txt", "a") as f:
        f.write(f"{datetime.now()} - {msg}\n")


# ================= DB CONNECTION =================
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Root@123"
)
cursor = conn.cursor()

cursor.execute("CREATE DATABASE IF NOT EXISTS inventory_pipeline")
cursor.execute("USE inventory_pipeline")


# ================= CREATE TABLES =================
def create_tables():
    # Bronze (raw data)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS bronze (
        id INT AUTO_INCREMENT PRIMARY KEY,
        raw_data TEXT,
        ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # Silver (clean data)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS silver (
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

    # Gold (final summary)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS gold (
        sku VARCHAR(50),
        restock_qty INT,
        urgency VARCHAR(20),
        stockout_date DATE,
        priority_score FLOAT
    )
    """)

    conn.commit()
    log("Tables created")


# ================= EXTRACT =================
def extract_data():
    try:
        stock = pd.read_csv("data/Warehouse_Stock.csv")
        sales = pd.read_csv("data/Sales_Velocity.csv")
        log("Data extracted")
        return stock, sales
    except Exception as e:
        log(f"Error in extract: {e}")
        return None, None


# ================= LOAD BRONZE =================
def load_bronze(df):
    query = "INSERT INTO bronze (raw_data) VALUES (%s)"

    data = [
        (json.dumps(row.to_dict()),)
        for _, row in df.iterrows()
    ]

    cursor.executemany(query, data)
    conn.commit()
    log("Bronze layer loaded")


# ================= TRANSFORM =================
def transform_data(stock, sales):
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

    # Restock
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

    log("Data transformed (Silver ready)")
    return df


# ================= LOAD SILVER =================
def load_silver(df):
    query = """
    INSERT INTO silver
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
    log("Silver layer loaded")


# ================= LOAD GOLD =================
def load_gold(df):
    cursor.execute("DELETE FROM gold")

    data = [
        (
            row["sku"],
            int(row["restock_qty"]),
            row["urgency"],
            row["stockout_date"],
            float(row["priority_score"])
        )
        for _, row in df.iterrows()
    ]

    query = """
    INSERT INTO gold
    (sku, restock_qty, urgency, stockout_date, priority_score)
    VALUES (%s, %s, %s, %s, %s)
    """

    cursor.executemany(query, data)
    conn.commit()
    log("Gold layer loaded")


# ================= OUTPUT =================
def generate_output(df):
    os.makedirs("output", exist_ok=True)

    result = df[[
        "sku", "restock_qty", "urgency",
        "stockout_date", "priority_score"
    ]].to_dict(orient="records")

    with open("output/Purchase_Order.json", "w") as f:
        json.dump(result, f, indent=4)

    log("JSON output generated")


# ================= PIPELINE =================
def run_pipeline():
    log("Pipeline started")

    create_tables()

    stock, sales = extract_data()

    if stock is None:
        log("Stopping pipeline due to error")
        return

    # Bronze
    df_raw = pd.merge(stock, sales, on="sku")
    load_bronze(df_raw)

    # Silver
    df_clean = transform_data(stock, sales)
    load_silver(df_clean)

    # Gold
    load_gold(df_clean)

    # Output
    generate_output(df_clean)

    log("Pipeline completed")
    print("✅ Inventory Pipeline Completed")


# ================= START =================
if __name__ == "__main__":
    run_pipeline()
