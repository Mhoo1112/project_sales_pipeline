# 📊 Project Sales Pipeline

โปรเจกต์นี้เป็นตัวอย่างการสร้าง **Data Pipeline ด้วย Python** สำหรับวิเคราะห์ข้อมูลยอดขายจากหลายแหล่ง  
และแปลงให้อยู่ในรูปแบบที่พร้อมใช้งานต่อ เช่น ทำ Pivot, รวมรายเดือน, แปลงสกุลเงิน และ export ไฟล์

---

## 🛠 Tools & Libraries

- Python 3.10+
- Pandas
- SQLAlchemy
- dotenv
- requests
- pyarrow / fastparquet
- Google Cloud Storage (GCS)
- Git

---

## 🧱 โครงสร้างโปรเจกต์

project_sales_pipeline/
├── dags/ # DAGs สำหรับ Apache Airflow (optional)
├── data/ # ไฟล์ .csv / ข้อมูลดิบ
├── scripts/ # สคริปต์ Python สำหรับ ETL
├── tests/ # ไฟล์ test สำหรับตรวจสอบ logic
├── run_pipeline.py # สคริปต์หลักที่รันทั้ง pipeline
├── requirements.txt # รายการไลบรารีที่ใช้
└── README.md # ไฟล์แนะนำโปรเจกต์นี้


---

## 🔄 Workflow (ETL Pipeline)

1. **Extract**
    - อ่านไฟล์ยอดขาย `.csv` จากโฟลเดอร์ `/data/sales_files/`
    - ดึงอัตราแลกเปลี่ยน USD → THB จาก API

2. **Transform**
    - รวมยอดขายรายลูกค้า
    - แปลงสกุลเงิน
    - สร้าง Pivot Table

3. **Load**
    - Export ไฟล์ `.csv` และ `.parquet`
    - (สามารถต่อยอดเชื่อมกับ BigQuery หรือ GCS ได้)


---

## ▶️ วิธีการรันโปรเจกต์

```bash
pip install -r requirements.txt
python run_pipeline.py
```