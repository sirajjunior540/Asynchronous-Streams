import asyncio
import time
from datetime import datetime

import websockets
import jwt
from fastapi import FastAPI, HTTPException

SECRET_KEY = "your-secret-key"
ALGORITHM = "HS256"

app = FastAPI()


async def test_websocket(token: str):
    uri = f"ws://localhost:8000/ws/{token}"

    async with websockets.connect(uri) as websocket:
        start_time = datetime.now().strftime("%H:%M:%S")
        try:
            while True:
                message = await websocket.recv()
                print("Received:", message)
        except websockets.exceptions.ConnectionClosed:
            finish_time = datetime.now().strftime("%H:%M:%S")
            print(f"Connection closed. Start time: {start_time}, Finish time: {finish_time}")


@app.get("/start-consumer")
async def start_consumer():
    token = create_jwt_token()
    asyncio.create_task(test_websocket(token))
    return {"message": "WebSocket consumer started"}


def create_jwt_token():
    payload = {
        "sub": "testuser",
        "iat": time.time(),
        "exp": time.time() + 600  # Token valid for 10 minutes
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)
