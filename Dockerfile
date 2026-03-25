FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py fetch.py storage.py transform.py generate.py incidents.py orchestrator.py \
     __init__.py __main__.py cities.csv pipeline/

CMD ["python", "-u", "-m", "pipeline"]
