import asyncio
import websockets
import socket
import json
import sys
import os


async def check_backend():
    hostname = socket.gethostname()
    bind_address = os.getenv("WS_BIND_ADDRESS", hostname if hostname else "localhost")
    port = "8765"
    uri = f"ws://{bind_address}:{port}"

    try:
        # Connection-Timeout 3 seconds, to avoid hanging if backend is not running
        async with asyncio.timeout(3):
            async with websockets.connect(uri) as websocket:
                # Wait for "Hello" message from backend
                response = await websocket.recv()
                data = json.loads(response)

                if data.get("type") == "Hello":
                    sys.exit(0)
                else:
                    sys.exit(1)
    except Exception:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(check_backend())
