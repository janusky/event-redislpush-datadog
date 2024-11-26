# Dockerfile for Producer
FROM python:3.9

WORKDIR /app

COPY producer.py .

RUN pip install redis requests flask

CMD ["python", "producer.py"]