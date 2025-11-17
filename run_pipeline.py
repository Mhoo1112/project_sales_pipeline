# # ดึงไฟล์ยอดขายจาก CSV:
# from scripts.extract_csv import extract_sales_csv
# folder_path = r'C:/Users/Buath/projects/project_sales_pipeline/data/sales_files'
# df = extract_sales_csv(folder_path)
#
# # ดึงอัตราแลกเปลี่ยน USD → THB จาก API:
# from scripts.fetch_exchange_rate import fetch_usd_to_thb
# rate = fetch_usd_to_thb()
#
# # ดึงข้อมูลจาก MySQL (join customers + products):
# from scripts.extract_mysql import extract_mysql
# df_qurey = extract_mysql()
#
# # สรุปยอดขายจาก MySQL:
# from scripts.transform_mysql_data import transform_mysql_data
# df_01 = transform_mysql_data(df_qurey)
#
# # รวมข้อมูล Report:
# from scripts.transform_final_report import transform_final_report
# final_df = transform_final_report(df, df_01, rate)

# Import ฟังก์ชันที่เขียนไว้
# ดึงไฟล์ยอดขายจาก CSV:
from scripts.extract_csv import extract_sales_csv
sales_df = extract_sales_csv()

# ดึงอัตราแลกเปลี่ยน USD → THB จาก API:
from scripts.fetch_exchange_rate import fetch_usd_to_thb
exchange_rate = fetch_usd_to_thb()

# ดึงข้อมูลจาก MySQL
from scripts.extract_mysql import extract_mysql
mysql_df = extract_mysql()

# สรุปยอดขายจาก MySQL:
from scripts.transform_mysql_data import transform_mysql_data
transformed_mysql_df = transform_mysql_data(mysql_df)

# รวมข้อมูล Report:
from scripts.transform_final_report import transform_final_report
final_report_df = transform_final_report(sales_df, transformed_mysql_df, exchange_rate)

# รวมข้อมูล upload_to_gcs
from scripts.upload_to_gcs import upload_to_gcs
upload_to_gcs()

# # รวมข้อมูล upload_to_bq
# from scripts.load_to_bigquery import load_to_bigquery
# load_to_bigquery = load_to_bigquery()

# ส่งอีเมล
from scripts.send_email_report import send_email_report
send_email_report()