FROM python:3.11-bullseye

WORKDIR /app

COPY worker.py /app/
COPY requirements.worker.txt /app/

RUN pip install --no-cache-dir -r requirements.worker.txt

EXPOSE 80

RUN python worker.py --init

CMD ["uvicorn", "worker:app", "--host", "0.0.0.0", "--port", "80"]
