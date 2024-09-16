FROM python:3.11-bullseye

WORKDIR /app

COPY requirements.worker.txt /app/
RUN pip install --no-cache-dir -r requirements.worker.txt

COPY worker.py /app/

EXPOSE 8000

# RUN python worker.py --init

ENTRYPOINT [ "python", "worker.py", "--master", "0.0.0.0" ]
# CMD ["uvicorn", "worker:app", "--host", "0.0.0.0", "--port", "80"]
