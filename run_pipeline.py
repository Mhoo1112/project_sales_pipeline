# ดึงไฟล์ยอดขายจาก CSV:
from scripts.extract_csv import extract_sales_csv
folder_path = r'C:/Users/Buath/projects/project_sales_pipeline/data/sales_files'
df = extract_sales_csv(folder_path)

# ดึงอัตราแลกเปลี่ยน USD → THB จาก API:
from scripts.fetch_exchange_rate import fetch_usd_to_thb
rate = fetch_usd_to_thb()

# ดึงข้อมูลจาก MySQL (join customers + products):
from scripts.extract_mysql import extract_mysql
df_qurey = extract_mysql()

# สรุปยอดขายจาก MySQL:
from scripts.transform_mysql_data import transform_mysql_data
df_01 = transform_mysql_data(df_qurey)

# รวมข้อมูล Report:
from scripts.transform_final_report import transform_final_report
final_df = transform_final_report(df, df_01, rate)