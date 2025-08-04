import os
from dotenv import load_dotenv
from scripts.log_start_log_end import log_start
from scripts.log_start_log_end import log_end

log_start()

# เชื่อมต่อกับโฟลเดอร์ .env
path_dotenv = os.path.join(
    os.path.dirname(__file__),"..","config",".env")
# โหลดข้อมูลจาก .env
load_dotenv(path_dotenv)



log_end()