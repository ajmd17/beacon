FROM python:3.11-bullseye

WORKDIR /app

COPY requirements.master.txt /app/

RUN pip install --no-cache-dir -r requirements.master.txt

COPY master.py /app/

EXPOSE 8000

CMD ["uvicorn", "master:app", "--host", "0.0.0.0", "--port", "8000"]
