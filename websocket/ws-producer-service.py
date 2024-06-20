from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
import httpx
import asyncio
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

client = httpx.AsyncClient()
limiter = Limiter(key_func=get_remote_address)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    yield
    # Shutdown logic
    await client.aclose()


app = FastAPI(lifespan=lifespan)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# app.state.limiter = limiter
origins = [
    "*",

]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# Secret key for JWT encoding/decoding
SECRET_KEY = "your-secret-key"
ALGORITHM = "HS256"


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


async def fetch_url(client, url):
    try:
        response = await client.get(url)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=str(e))


def decode_jwt_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")


@app.websocket("/ws/{token}")
# @limiter.limit("1/minute")
async def websocket_endpoint(websocket: WebSocket, token: str):
    print("start time:", datetime.now().strftime("%H:%M:%S"))
    payload = decode_jwt_token(token)
    await websocket.accept()
    urls = [f"https://jsonplaceholder.typicode.com/posts/{i}" for i in range(1, 31)]

    tasks = [fetch_url(client, url) for url in urls]

    try:
        for task in asyncio.as_completed(tasks):
            result = await task
            await websocket.send_json(result)
            await asyncio.sleep(3)  # Adding delay for testing purposes
    except WebSocketDisconnect:
        print("WebSocket disconnected")
    finally:
        await websocket.close()


@app.exception_handler(RateLimitExceeded)
async def rate_limit_exceeded_handler(request, exc):
    return JSONResponse(
        status_code=429,
        content={"detail": "Rate limit exceeded"},
    )

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
