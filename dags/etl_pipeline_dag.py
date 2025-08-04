from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.extract_csv import extract_csv

# ✅ Import ฟังก์ชันที่คุณเขียนไว้
# ดึงไฟล์ยอดขายจาก CSV:
from scripts.extract_csv import extract_sales_csv
# ดึงอัตราแลกเปลี่ยน USD → THB จาก API:
from scripts.fetch_exchange_rate import fetch_usd_to_thb
# ดึงข้อมูลจาก MySQL (join customers + products):
from scripts.extract_mysql import extract_mysql
# สรุปยอดขายจาก MySQL:
from scripts.transform_mysql_data import transform_mysql_data
# รวมข้อมูล Report:
from scripts.transform_final_report import transform_final_report
# รวมข้อมูล Report:
from scripts.upload_to_gcs import upload_to_gcs

with DAG("etl_pipeline_dag",
         description="ETL pipeline: CSV + API + MySQL → Final Report",
         start_date=datetime(2024, 1, 1),
         schedule_interval=None,
         catchup=False,
         tags=["etl", "project-sales"]) as dag:

    t1 = PythonOperator(
        task_id="extract_sales_csv",
        python_callable=extract_sales_csv,
    )

    t2 = PythonOperator(
        task_id="fetch_usd_to_thb",
        python_callable=fetch_usd_to_thb,
    )

    t3 = PythonOperator(
        task_id="extract_mysql",
        python_callable=extract_mysql,
    )

    t4 = PythonOperator(
        task_id="transform_mysql_data",
        python_callable=transform_mysql_data,
    )

    t5 = PythonOperator(
        task_id="transform_final_report",
        python_callable=transform_final_report,
    )

    t6 = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
    )

    [t1, t2, t3] >> t4
    t4 >> t5 >> t6

    # t1: extract_sales_csv
    # t2: fetch_usd_to_thb
    # t3: extract_mysql
    #            │
    #            ▼
    #         t4: transform_mysql_data
    #            │
    #            ▼
    #         t5: transform_final_report
    #            │
    #            ▼
    #         t6: upload_to_gcs