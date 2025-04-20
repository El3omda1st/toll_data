# ETL Toll Data Pipeline with Apache Airflow

This project implements an end-to-end ETL pipeline using **Apache Airflow** to automate the ingestion, transformation, and consolidation of toll data from multiple file formats. It demonstrates how to orchestrate data workflows using Airflow operators, task dependencies, and Python functions.

---

## 🚀 Project Overview

The pipeline performs the following tasks:

1. **Download** a compressed data archive containing CSV, TSV, and fixed-width files.
2. **Extract** relevant columns from each file format:
   - `CSV`: Vehicle data
   - `TSV`: Toll plaza data
   - `Fixed-width`: Payment data
3. **Consolidate** all extracted data into a single dataset.
4. **Transform** the data by converting vehicle types to uppercase.
5. Save intermediate and final outputs into a `staging/` directory.

---

## 🛠️ Technologies Used

- **Apache Airflow** (via Docker)
- **Python** (pandas, os)
- **BashOperator & PythonOperator**
- **Pandas** for data processing
- **Docker & Docker Compose** for environment setup

---

## 🗂️ Project Structure

```
airflow_docker_train/
│
├── dags/
│   ├── ETL_toll_data.py         # Main Airflow DAG
│   └── python_etl/
│       ├── staging/             # Contains raw, intermediate, and output CSVs
│       │   ├── csv_data.csv
│       │   ├── tsv_data.csv
│       │   ├── fixed_width_data.csv
│       │   ├── extracted_data.csv
│       │   └── transformed_data.csv
│       └── DWH/
│           └── Create_Tables.sql  # (Optional) Schema SQL file
│
├── docker-compose.yaml          # Docker setup for Airflow
├── plugins/                     # (Empty or custom Airflow plugins)
└── requirements.txt             # Python dependencies
```

---

## 📅 DAG Overview

The DAG is scheduled to run **daily** and consists of the following tasks:

```
download_data
      ↓
  unzip_data
      ↓
 ┌────────┬──────────┬──────────────┐
 │extract_csv │ extract_tsv │ extract_fixed│
 └────┬───────┴──────┬───────┘
      ↓              ↓
     consolidate_data
              ↓
       transform_data
```

---

## ⚙️ How to Run

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/airflow_docker_train.git
cd airflow_docker_train
```

### 2. Start Airflow Using Docker Compose

```bash
docker-compose up --build
```

### 3. Access the Airflow Web UI

- Navigate to [http://localhost:8080](http://localhost:8080)
- Login with the default credentials (usually `airflow` / `airflow`)
- Enable and trigger the `ETL_toll_data` DAG

---

## ✅ Output

The final cleaned and transformed data is saved as:

```bash
/opt/airflow/dags/python_etl/staging/transformed_data.csv
```

---


## 📝 License

This project is for educational purposes as part of the **IBM Data Engineering Capstone**.
