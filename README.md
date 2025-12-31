

---

# 📊 Excel to MySQL CDC Pipeline

A **Python-based batch ETL & CDC (Change Data Capture) pipeline** that ingests data from an Excel file into MySQL with:

* **Historic layer** (append-only, full audit)
* **CDC layer** (latest active snapshot)
* **Archive layer** (file-level metadata tracking)

This project demonstrates **real-world data engineering concepts** such as CDC, layered storage, auditability, and batch ingestion.

---

## 🏗️ High-Level Architecture

```
Excel File (.xlsx)
        │
        ▼
Python ETL Script (Pandas + CDC Logic)
        │
        ▼
MySQL Database
 ├── historic_layer  (Full History)
 ├── cdc_layer       (Latest Snapshot)
 └── archive_layer   (Metadata)
```

---

## 📂 Project Folder Structure

```
excel-mysql-cdc-pipeline/
│
├── data/
│   ├── raw/
│   │   └── referrals_data.xlsx
│   │
│   └── archive/
│
├── src/
│   └── main.py
│
├── config/
│   └── .env.example
│
├── logs/
│
├── requirements.txt
├── README.md
└── .gitignore
```

---

## 📁 Folder Description

| Folder/File           | Purpose                       |
| --------------------- | ----------------------------- |
| `data/raw`            | Source Excel files            |
| `data/archive`        | Logical archive reference     |
| `src/main.py`         | Main ETL + CDC logic          |
| `config/.env.example` | Environment variable template |
| `logs`                | Execution logs (optional)     |
| `requirements.txt`    | Python dependencies           |
| `README.md`           | Project documentation         |

---

## ⚙️ Prerequisites

Before running the project, ensure you have:

* Python **3.8+**
* MySQL **5.7+ / 8.x**
* Git
* MySQL user with CREATE & INSERT privileges

---

## 🧰 Python Dependencies

Install required libraries using:

```bash
pip install -r requirements.txt
```

### `requirements.txt`

```txt
pandas
mysql-connector-python
python-dotenv
openpyxl
```

---

## 🔐 Environment Configuration

Create a `.env` file inside the `config/` directory.

### `.env.example`

```env
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=your_password
DB_NAME=cdc_db
EXCEL_FILE=data/raw/referrals_data.xlsx
```

📌 **Important**

* Do NOT commit `.env` to GitHub
* Add `.env` to `.gitignore`

---

## 🗃️ Database Schema Design

### Common Schema Used

```sql
S_n0              INT
Refferal Person   VARCHAR(100)
LinkedIn          VARCHAR(255)
Company Name      VARCHAR(150)
Career Portal     VARCHAR(255)
Year              INT
Statusflag        CHAR(1)
Timestamp         DATETIME
load_ts           DATETIME (auto)
```

---

## 🗄️ Database Tables

### 1️⃣ `historic_layer`

* Append-only table
* Stores **every version** of every record
* Used for:

  * Audit
  * Time travel
  * Debugging

---

### 2️⃣ `cdc_layer`

* Stores **latest active record per business key**
* Primary Key: `S_n0`
* Refreshed on every run

---

### 3️⃣ `archive_layer`

Tracks ingestion metadata:

| Column       | Description                 |
| ------------ | --------------------------- |
| file_name    | Source file name            |
| processed_at | Load timestamp              |
| record_count | Number of records processed |

---

## 🔁 Step-by-Step Execution Flow

### ✅ Step 1: Load Environment Variables

* Reads database credentials
* Reads Excel file path

```python
load_dotenv()
```

---

### ✅ Step 2: Read Excel File

* Loads Excel into Pandas DataFrame
* Converts `Timestamp` column to datetime

```python
raw_df = pd.read_excel(RAW_EXCEL_FILE)
raw_df["Timestamp"] = pd.to_datetime(raw_df["Timestamp"])
```

---

### ✅ Step 3: Connect to MySQL

* Uses `mysql-connector-python`
* Opens cursor for execution

---

### ✅ Step 4: Create Tables (If Not Exists)

* `historic_layer`
* `cdc_layer`
* `archive_layer`

Ensures idempotent execution.

---

### ✅ Step 5: Load Historic Layer

* Inserts **all incoming records**
* No updates or deletes

```text
Excel → historic_layer
```

📌 Purpose: Maintain full data history.

---

### ✅ Step 6: Load Existing CDC Data

* Reads current snapshot from `cdc_layer`
* Converts it into Pandas DataFrame

---

### ✅ Step 7: Apply CDC Logic

**CDC Rules Implemented:**

1. Combine existing CDC + new data
2. Sort by:

   * Business Key (`S_n0`)
   * Timestamp (DESC)
3. Keep latest record per key
4. Exclude deleted records (`Statusflag = 'D'`)

```text
Latest record wins
Deleted records removed
```

---

### ✅ Step 8: Refresh CDC Layer

* Truncates `cdc_layer`
* Inserts latest active records

📌 CDC layer always represents **current state**

---

### ✅ Step 9: Archive File Metadata

* Stores:

  * File name
  * Processing time
  * Record count

Used for monitoring & audit.

---

### ✅ Step 10: Close Connections

* Safely closes cursor and DB connection

---

## ▶️ How to Run the Project

```bash
python src/main.py
```

Expected output:

```text
Connected to MySQL
Tables ensured
Historic layer updated
CDC layer refreshed
File archived in DB
MySQL connection closed
```

---

## 🎯 Key Data Engineering Concepts Demonstrated

* Batch ETL
* Change Data Capture (CDC)
* Layered data modeling
* Append-only history
* Current-state snapshot
* Metadata & audit tracking
* Idempotent table creation

---

## 🚀 Future Enhancements

* Incremental file ingestion
* File checksum validation
* Airflow orchestration
* Spark-based CDC
* Cloud migration (S3 + RDS)
* Data quality checks

---

## 🧾 Summary

> Designed and implemented a Python-based batch CDC pipeline ingesting Excel data into MySQL with historical tracking, current-state snapshots, and audit metadata using Pandas and SQL.

---

## 👤 Author

**Yelleti Sudheer Kumar**
Big Data & Data Engineering Enthusiast

---

