import os
import requests
import pandas
from dotenv import load_dotenv
from scripts.log_start_log_end import log_start
from scripts.log_start_log_end import log_end

def fetch_usd_to_thb():
    """
        อ่านข้อมูล Convertion Rate จาก API/
    """
    log_start()

    # โหลด .env ที่อยู่ในโฟลเดอร์ config
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', 'config', '.env')
    load_dotenv(dotenv_path)

    # URL สำหรับเรียกดูอัตราแลกเปลี่ยน
    url = os.getenv("EXCHANGE_RATE_API")
    print("✅ DEBUG URL:", url)  # ตรวจสอบว่าโหลดได้จริง

    # โหลดข้อมูลจาก API
    response = requests.get(url)
    # แปลง ข้อมูล JSON ที่ได้จาก API ให้กลายเป็น Python dictionary
    exchange_rate_data = response.json()

    # thb_rate = exchange_rate_data["conversion_rates"]["THB"]
    if "conversion_rates" not in exchange_rate_data:
        raise KeyError("❌ ไม่พบ key 'conversion_rates' ใน response")
    # เมื่อ raise ถูกเรียก → โปรแกรม หยุดทันที → โค้ดหลังจากนั้นจะ ไม่รัน

    exchange_df = pandas.DataFrame.from_dict(
        exchange_rate_data["conversion_rates"],  # ดึง dict ของอัตรา
        orient="index",  # ให้ key เป็น indexแต่ละ key ใน dictionary จะกลายเป็น index (แถว) แต่ละ value จะกลายเป็น ค่าของแถว นั้น
        columns=["rate"]  # ตั้งชื่อ column เป็น rate
    ).reset_index().rename(columns={"index": "currency"})
    # exchange_df = pandas.DataFrame(
    #     exchange_rate_data["conversion_rates"].items(),  # → List of tuples
    #     columns=["currency", "rate"]
    # )

    from datetime import datetime
    time_str = exchange_rate_data["time_last_update_utc"]

    formats = [
        "%a, %d %b %Y %H:%M:%S %z",
        "%d %b %Y %H:%M:%S",
        "%d %m %Y %H:%M:%S",
        "%Y %m %d %H:%M:%S",
        "%Y %m %d",
        "%d %m %Y"
    ]
    for fmt in formats:
        try:
            time_obj = datetime.strptime(time_str, fmt)
            break  # ✅ หยุดลูปถ้าแปลงสำเร็จ
        except ValueError:
            continue
    else:
        raise ValueError(f"❌ ไม่สามารถแปลงรูปแบบของวันที่ได้: {time_str}")
    # try:
    #     time_obj = datetime.strptime(time_str, "%a, %d %b %Y %H:%M:%S %z")
    # except ValueError:
    #     try:
    #         time_obj = datetime.strptime(time_str, "%d %b %Y %H:%M:%S")
    #     except ValueError:
    #         try:
    #             time_obj = datetime.strptime(time_str, "%d %m %Y %H:%M:%S")
    #         except ValueError:
    #             try:
    #                 time_obj = datetime.strptime(time_str, "%Y %m %d %H:%M:%S")
    #             except ValueError:
    #                 try:
    #                     time_obj = datetime.strptime(time_str, "%Y %m %d")
    #                 except ValueError:
    #                     time_obj = datetime.strptime(time_str, "%d %m %Y")
    # 🔍 อธิบาย format:
    # %a = ชื่อวัน (Sat)
    # %d = วันที่ (26)
    # %b = เดือนแบบตัวย่อ (Jul)
    # %Y = ปี (2025)
    # %H:%M:%S = เวลา
    # %z = timezone offset (+0000)

    # from scripts.parse_datetime import parse_datetime
    # time_obj = parse_datetime(exchange_rate_data["time_last_update_utc"])

    exchange_df["date"] = time_obj.strftime("%Y-%m-%d")

    # from datetime import datetime
    # exchange_df["date"] = datetime.now()
    # exchange_df["date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # แปลง index ให้กลายเป็น column       # ตั้งชื่อ column ใหม่
    print("\n✅ rate:")
    print(exchange_df)

    if "THB" not in exchange_rate_data["conversion_rates"]:
        raise KeyError("❌ ไม่พบค่า THB ใน conversion_rates")
    # เมื่อ raise ถูกเรียก → โปรแกรม หยุดทันที → โค้ดหลังจากนั้นจะ ไม่รัน

    # ✅ Filter ค่า THB จาก DataFrame
    thb_df = exchange_df[exchange_df["currency"] == "THB"].reset_index(drop=True)
    thb_value = thb_df["rate"].values[0]
    print("\n อัตรา THB จาก DataFrame:")
    print(thb_df)

    try:
        output_path = r'C:/Users/Buath/projects/project_sales_pipeline/output'
        os.makedirs(output_path, exist_ok=True)
        output_file_parquet = os.path.join(output_path, "fetch_exchange_rate.parquet")
        output_file_CSV = os.path.join(output_path, "fetch_exchange_rate.csv")

        exchange_df.to_parquet(output_file_parquet,index=False)
        print(f"\n✅ Saved parquet file to {output_file_parquet}")
        exchange_df.to_csv(output_file_CSV, index=False)
        print(f"✅ Saved csv file to {output_file_CSV}")
    except Exception as e:
        print("❌ เกิดข้อผิดพลาด:", e)

    log_end()
    return thb_value