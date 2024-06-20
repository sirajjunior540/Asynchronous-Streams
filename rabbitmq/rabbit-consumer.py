from fastapi import FastAPI
from contextlib import asynccontextmanager
import pika
import json
import asyncio


# Configure RabbitMQ
RABBITMQ_HOST = 'localhost'
RABBITMQ_QUEUE = 'test_queue'

# Global variables for RabbitMQ connection and channel
connection = None
channel = None


def callback(ch, method, properties, body):
    message = json.loads(body)
    print(f"Received {message}")


def start_consuming():
    global channel
    channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global connection, channel
    # Startup logic
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE)
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, start_consuming)
    yield
    # Shutdown logic
    if channel and connection:
        channel.close()
        connection.close()


app = FastAPI(lifespan=lifespan)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8001)
