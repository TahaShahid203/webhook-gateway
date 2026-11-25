FROM python:3.12-slim
WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app app
RUN mkdir -p /var/log/edge
CMD ["uvicorn", "app.main:build_default_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
