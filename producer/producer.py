import redis
import json
import os
from flask import Flask, request

# Conectar a Redis
REDIS_HOST = os.getenv('REDIS_HOST')
redis_client = redis.StrictRedis(host=REDIS_HOST, port=6379, db=0)

app = Flask(__name__)


@app.route('/event', methods=['POST'])
def receive_event():
    event_data = request.json
    if event_data:
        # Almacenar el evento en Redis
        redis_client.rpush('event_queue', json.dumps(event_data))
        print(f"Received and stored event: {event_data}")
        return {'status': 'success', 'message': 'Event stored'}, 200
    else:
        return {'status': 'error', 'message': 'Invalid data'}, 400


if __name__ == "__main__":
    # Ejecutar el servidor en el puerto 5000
    app.run(host='0.0.0.0', port=5000)
