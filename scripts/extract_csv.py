import os
import pandas
from scripts.log_start_log_end import log_start
from scripts.log_start_log_end import log_end

def extract_sales_csv():
    """
        อ่านไฟล์ CSV ทั้งหมดจากโฟลเดอร์ sales_files/
        - รวมเป็น DataFrame เดียว
        - ปรับชื่อคอลัมน์ให้เป็นมาตรฐาน
        - คืนค่า DataFrame
        """
    # folder_path = r'C:/Users/Buath/projects/project_sales_pipeline/data/sales_files'
    log_start()
    # ✅ 1. สร้างลิสต์ไว้รวมทุก DataFrame
    all_files = []
    folder_path = r'C:/Users/Buath/projects/project_sales_pipeline/data/sales_files'
    # ✅ 2. วนลูปอ่านทุกไฟล์ที่ลงท้ายด้วย .csv
    for filename in os.listdir(folder_path):
        if filename.startswith("2024_sales") and filename.endswith(".csv"):

            # ✅ 3. อ่าน CSV แบบไม่เช็ค header ตายตัว (เพราะชื่อคอลัมน์อาจไม่เหมือนกัน)
            path_file = os.path.join(folder_path, filename)
            try:
                data_frame = pandas.read_csv(path_file)
                # print("------------------------")
                print("\n▶️path_file")
                print(path_file)

                # ✅ 4. แปลงชื่อคอลัมน์ให้เป็นมาตรฐาน (เล็กหมด, ไม่มีช่องว่าง)
                new_columns = []
                for col in data_frame.columns:
                    col = col.strip()
                    col = col.replace(' ', '_')
                    col = col.lower()
                    new_columns.append(col)
                # print("\n▶️new_columns")
                # print(new_columns)
                data_frame.columns = new_columns
                # ✅ 5. แปลงชื่อ column ที่ผิดให้ตรงกัน (mapping)
                data_frame = data_frame.rename(
                    columns={
                        "orderid": "order_id",
                        "orderdate": "order_date",

                    }
                )
                # ✅ 5.1 เรียง column
                # data_frame = data_frame[sorted(data_frame.columns)] เรียงจากน้อยไปมาก
                required_cols = ['order_id', 'order_date', 'customer_id', 'product_id', 'amount_usd']
                # ตรวจสอบคอลัมน์ที่ขาด
                ordered_cols = []
                # วนลูปเอาคอลัมน์ตามลำดับที่ต้องการก่อน
                for col in required_cols:
                    if col in data_frame.columns:
                        ordered_cols.append(col)
                # วนลูปเพิ่มคอลัมน์อื่น ๆ ที่เหลือ
                for col in data_frame.columns:
                    if col not in required_cols:
                        ordered_cols.append(col)
                data_frame = data_frame[ordered_cols]
                # ----------------------------------------------------------------------------------------------------------
                # missing_cols =[]
                # for col in required_cols:
                #     if col not in data_frame.columns:
                #         missing_cols.append(col)
                # # ถ้าไม่มีคอลัมน์ที่ขาดเลย
                # if len(missing_cols) == 0:
                #     data_frame = data_frame[required_cols]
                # else:
                #     print(f"❌ Missing column(s) in file: {filename} → {missing_cols}")
                #     continue
                # ----------------------------------------------------------------------------------------------------------
                # data_frame = data_frame[[
                #     'order_id',
                #     'order_date',
                #     'customer_id',
                #     'product_id',
                #     'amount_usd'
                # ]]
                # ✅ 5.2 เปลี่ยนประเภทข้อมูลของคอลัมน์
                data_frame["order_date"] = pandas.to_datetime(data_frame["order_date"], errors="coerce")
                data_frame["order_date"] = pandas.to_datetime(data_frame["order_date"].dt.strftime("%Y-%m-%d"),
                                                              errors="coerce")
                data_frame["amount_usd"] = pandas.to_numeric(data_frame["amount_usd"], errors="coerce")
                # data_frame["amount_usd"] = data_frame["amount_usd"].fillna(0)

                print("\n▶️DataFrame of each file")
                print(data_frame)
                print(f"✅ Loaded: {filename} | {len(data_frame)} rows x {len(data_frame.columns)} cols")

                if not data_frame.empty:
                    all_files.append(data_frame)
                else:
                    print(f"! Empty file: {filename}")
            except Exception as e:
                print(f"❌ Error loading file {filename}: {e}")
                continue
    # ✅ 7. รวมทุก DataFrame เข้าด้วยกัน
    if all_files:
        combined_df = pandas.concat(all_files, ignore_index=True)
        combined_df = combined_df.drop_duplicates().reset_index(drop=True)
        # combined_df = combined_df.dropna(subset=["customer_id"])
        combined_df = combined_df.dropna().reset_index(drop=True)
        print("\n ️✅dtypes")
        print(combined_df.dtypes)
        print("\n ️✅count")
        print(combined_df.count())
        print("\n ️✅DataFrame Combined --------------------")
        print(combined_df)

        # ✅ 8. บันทึกเป็นไฟล์ CSV vs Parquet
        output_path = r'C:/Users/Buath/projects/project_sales_pipeline/output'
        output_file_parquet = os.path.join(output_path,"cleaned_sales.parquet")
        output_file_CSV = os.path.join(output_path,"cleaned_sales.csv")

        combined_df.to_parquet(output_file_parquet)
        print(f"\n✅ Saved parquet file to {output_file_parquet}")
        combined_df.to_csv(output_file_CSV, index=False)
        print(f"✅ Saved csv file to {output_file_CSV}")

    else:
        combined_df = pandas.DataFrame()  # ถ้าไม่มีไฟล์เลย
        print("\n ️✅DataFrame --------------------")
        print(combined_df)

    log_end()
    return combined_df