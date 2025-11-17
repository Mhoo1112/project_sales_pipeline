FROM python:3.11-slim
WORKDIR /projects/project_sales
#COPY requirements.txt projects/project_sales
RUN cat > requirements.txt << 'EOF'
pandas
requests
python-dotenv
google-cloud-storage
google-cloud-bigquery
sqlalchemy
pymysql
cryptography
pyarrow
fastparquet
EOF
RUN pip install --no-cache-dir -r requirements.txt
RUN cat > app.py << 'EOF'
print("Hello")
EOF
CMD ["python", "app.py"]