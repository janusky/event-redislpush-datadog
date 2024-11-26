import redis
import urllib3
# from urllib3.exceptions import MaxRetryError
# from urllib3.util.retry import Retry
# import requests
import json
import os

# API Key de Datadog desde variables de entorno
DATADOG_API_KEY = os.getenv('DATADOG_API_KEY')
DATADOG_APP_KEY = os.getenv('DATADOG_APP_KEY', default="my-app-key-to-datadog")
DATADOG_HOST = os.getenv('DATADOG_HOST', default="localhost")
REDIS_HOST = os.getenv('REDIS_HOST', default="localhost")

# Conectar a Redis
redis_client = redis.StrictRedis(host=REDIS_HOST, port=6379, db=0)

# FIXME 25/11/2024 Implement
# # https://datadogpy.readthedocs.io/en/latest/
# from datadog import initialize, api
# initialize(
#     api_key=DATADOG_API_KEY,
#     app_key=DATADOG_APP_KEY,
#     statsd_host="127.0.0.1",
#     statsd_port=8125
# )


def send_to_datadog(event_data):
    # try:
    # localhost|127.0.0.1
    url = f"http://{DATADOG_HOST}:8126/v0.3/traces"

    # crear un grupo de conexiones
    http = urllib3.PoolManager(
        num_pools=2,              # número de grupos de conexiones a crear
        headers=None,             # sin cabeceras por defecto
        maxsize=3,                # máximo de conexiones a mantener en cada grupo
        block=True,               # bloquear si todas las conexiones están en uso
        timeout=30,               # tiempo de espera por la conexión
        retries=None,             # reintento de las peticiones fallidas
    )
    # Configure the Retry class with desired parameters
    # retries = Retry(
    #     total=4,  # Total number of allowed retries
    #     backoff_factor=0.5,  # Factor by which retry delays increase
    #     # HTTP status codes to retry
    #     status_forcelist=[500, 502, 503, 504],
    # )

    jsonStr = json.dumps(event_data, indent=2)  # .encode('utf-8')
    # retries=retries, timeout=3,
    response = http.request(method='PUT', url=url, body=jsonStr,
                            headers={'DD-API-KEY': DATADOG_API_KEY, 'Content-Type': 'application/json'})
    # Simple
    # response = requests.put(
    #     url,
    #     headers={'DD-API-KEY': DATADOG_API_KEY,
    #              'Content-Type': 'application/json'},
    #     data=jsonStr,
    #     verify=False,
    # )
    print(
        f"Consumed {url}, api_key {DATADOG_API_KEY}, data: {jsonStr}, Datadog Status: {response.status}")

    return response
    # except MaxRetryError as e:
    #     print(f"Error: Maximum retries exceeded. {e}")


def consume_events():
    print("Bloquea hasta que haya un evento ..")

    while True:
        # Esperar a que haya un nuevo evento
        # Bloquea hasta que haya un evento
        event = redis_client.brpop('event_queue', timeout=0)
        if event:
            # Procesar el evento
            event_data = json.loads(event[1])  # El evento viene como bytes
            data = [[{
                "title": "Nuevo evento recibido",
                "event": event_data,
                "priority": "normal"
            }]]
            # Enviar el evento a Datadog
            resp = send_to_datadog(data)


if __name__ == "__main__":
    consume_events()
