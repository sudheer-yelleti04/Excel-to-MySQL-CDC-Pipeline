import os
import pandas as pd
from mysql import connector
from dotenv import load_dotenv

# -------------------------------------------------
# Load Environment Variables
# -------------------------------------------------
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME")
}

RAW_EXCEL_FILE = os.getenv("EXCEL_FILE")

# -------------------------------------------------
# Schema (EXACT as requested)
# -------------------------------------------------
TABLE_SCHEMA = """
`S_n0` INT,
`Refferal Person` VARCHAR(100),
`LinkedIn` VARCHAR(255),
`Company Name` VARCHAR(150),
`Career Portal` VARCHAR(255),
`Year` INT,
`Statusflag` CHAR(1),
`Timestamp` DATETIME,
`load_ts` DATETIME DEFAULT CURRENT_TIMESTAMP
"""

# -------------------------------------------------
# Read Excel
# -------------------------------------------------
raw_df = pd.read_excel(RAW_EXCEL_FILE)
raw_df["Timestamp"] = pd.to_datetime(raw_df["Timestamp"])

# -------------------------------------------------
# MySQL Connection
# -------------------------------------------------
conn = connector.connect(**DB_CONFIG)
cursor = conn.cursor()

try:
    print("✅ Connected to MySQL")

    # -------------------------------------------------
    # Create Tables
    # -------------------------------------------------
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS historic_layer (
        {TABLE_SCHEMA}
    )
    """)

    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS cdc_layer (
        {TABLE_SCHEMA},
        PRIMARY KEY (`S_n0`)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS archive_layer (
        file_name VARCHAR(255),
        processed_at DATETIME,
        record_count INT
    )
    """)

    print("✅ Tables ensured")

    # -------------------------------------------------
    # 1️⃣ Insert into HISTORIC (append-only)
    # -------------------------------------------------
    historic_insert = """
    INSERT INTO historic_layer
    (`S_n0`,`Refferal Person`,`LinkedIn`,`Company Name`,
     `Career Portal`,`Year`,`Statusflag`,`Timestamp`)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """

    historic_data = list(
        raw_df[[
            "S_n0","Refferal Person","LinkedIn",
            "Company Name","Career Portal",
            "Year","Statusflag","Timestamp"
        ]].itertuples(index=False, name=None)
    )

    cursor.executemany(historic_insert, historic_data)
    conn.commit()

    print(f"🕓 Historic layer updated: {cursor.rowcount} rows")

    # -------------------------------------------------
    # 2️⃣ Load Existing CDC
    # -------------------------------------------------
    cursor.execute("SELECT * FROM cdc_layer")
    cols = [c[0] for c in cursor.description]
    existing_cdc_df = pd.DataFrame(cursor.fetchall(), columns=cols)

    # -------------------------------------------------
    # 3️⃣ Apply CDC Logic
    # -------------------------------------------------
    combined_df = pd.concat([existing_cdc_df, raw_df], ignore_index=True)

    combined_df = combined_df.sort_values(
        by=["S_n0", "Timestamp"], ascending=[True, False]
    )

    latest_df = combined_df.drop_duplicates(
        subset="S_n0", keep="first"
    )

    cdc_df = latest_df[latest_df["Statusflag"] != "D"]

    # -------------------------------------------------
    # 4️⃣ Refresh CDC Table
    # -------------------------------------------------
    cursor.execute("TRUNCATE TABLE cdc_layer")

    cdc_insert = """
    INSERT INTO cdc_layer
    (`S_n0`,`Refferal Person`,`LinkedIn`,`Company Name`,
     `Career Portal`,`Year`,`Statusflag`,`Timestamp`)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """

    cdc_data = list(
        cdc_df[[
            "S_n0","Refferal Person","LinkedIn",
            "Company Name","Career Portal",
            "Year","Statusflag","Timestamp"
        ]].itertuples(index=False, name=None)
    )

    cursor.executemany(cdc_insert, cdc_data)
    conn.commit()

    print(f"✅ CDC layer refreshed: {cursor.rowcount} rows")

    # -------------------------------------------------
    # 5️⃣ Archive File Metadata
    # -------------------------------------------------
    cursor.execute(
        """
        INSERT INTO archive_layer (file_name, processed_at, record_count)
        VALUES (%s, NOW(), %s)
        """,
        (os.path.basename(RAW_EXCEL_FILE), len(raw_df))
    )

    conn.commit()
    print("📦 File archived in DB")

except Exception as e:
    print("❌ Error:", e)

finally:
    cursor.close()
    conn.close()
    print("🔌 MySQL connection closed")
