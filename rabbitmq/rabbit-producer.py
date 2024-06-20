from fastapi import FastAPI, HTTPException
import httpx
import asyncio
import pika
import json
import logging
from contextlib import asynccontextmanager
from config import RABBITMQ_HOST, RABBITMQ_QUEUE, RABBITMQ_USER, RABBITMQ_PASSWORD, RABBITMQ_PORT

# Create a connection to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()
channel.queue_declare(queue=RABBITMQ_QUEUE)

client = httpx.AsyncClient()
logging.basicConfig(level=logging.INFO)

# RabbitMQ connection setup
credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
parameters = pika.ConnectionParameters(
    host=RABBITMQ_HOST,
    port=RABBITMQ_PORT,
    virtual_host='/',
    credentials=credentials
)





@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    yield
    # Shutdown logic
    await client.aclose()


app = FastAPI(lifespan=lifespan)


async def fetch_url(client, url):
    try:
        response = await client.get(url)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=str(e))


@app.get("/produce")
async def produce():
    urls = [f"https://jsonplaceholder.typicode.com/posts/{i}" for i in range(1, 90)]
    tasks = [fetch_url(client, url) for url in urls]
    results = await asyncio.gather(*tasks)

    for result in results:
        channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_QUEUE,
            body=json.dumps(result)
        )

    return {"message": "Messages sent to queue"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
