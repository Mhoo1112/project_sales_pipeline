from airflow import DAG
from airflow.decorators import task
from datetime import datetime

# Import ฟังก์ชันที่เขียนไว้
# ดึงไฟล์ยอดขายจาก CSV:
from scripts.extract_csv import extract_sales_csv
# ดึงอัตราแลกเปลี่ยน USD → THB จาก API:
from scripts.fetch_exchange_rate import fetch_usd_to_thb
# ดึงข้อมูลจาก MySQL
from scripts.extract_mysql import extract_mysql
# สรุปยอดขายจาก MySQL:
from scripts.transform_mysql_data import transform_mysql_data
# รวมข้อมูล Report:
from scripts.transform_final_report import transform_final_report
# รวมข้อมูล upload_to_gcs
from scripts.upload_to_gcs import upload_to_gcs
# ส่งเมล
from scripts.send_email_report import send_email_report
# รวมข้อมูล upload_to_bq

with DAG("etl_pipeline_01",
         description="ETL pipeline: CSV + API + MySQL → Final Report",
         start_date=datetime(2024, 1, 1),
         schedule='@hourly',
         catchup=False,
         tags=["etl", "project-sales"]) as dag:


    extract_sales_csv = task(extract_sales_csv)
    fetch_usd_to_thb = task(fetch_usd_to_thb)
    extract_mysql = task(extract_mysql)
    transform_mysql_data = task(transform_mysql_data)
    transform_final_report = task(transform_final_report)
    upload_to_gcs = task(upload_to_gcs)
    send_email_report = task(send_email_report)

    # ============================================================
    # Pipeline
    # ============================================================

    sales_df = extract_sales_csv()
    exchange_rate = fetch_usd_to_thb()
    mysql_df = extract_mysql()

    transformed_mysql_df = transform_mysql_data(mysql_df)
    final_report_df = transform_final_report(sales_df, transformed_mysql_df, exchange_rate)


    upload_to_gcs = upload_to_gcs()
    send_email_report = send_email_report()

    # -------------------------------------------------------------
    # กำหนด Dependencies
    # -------------------------------------------------------------

    final_report_df >> upload_to_gcs >> send_email_report