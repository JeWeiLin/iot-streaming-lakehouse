import os

# Superset 用來加密 session 與資料庫連線密碼，未設定時不會啟動
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
