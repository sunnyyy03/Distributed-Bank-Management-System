FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# Sleep command to temporarily keep the container alive
CMD ["python", "branch_client.py"]