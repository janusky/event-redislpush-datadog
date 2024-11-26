# Dockerfile for Consumer
FROM python:3.12

WORKDIR /app

COPY consumer.py .

# urllib3
RUN pip install redis requests

# Cargar variables de entorno
ENV DATADOG_API_KEY=your_actual_datadog_api_key

CMD ["python", "consumer.py"]