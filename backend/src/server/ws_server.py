# WS server

# from datetime import datetime
import asyncio
import websockets
import logging

logging.basicConfig()


class WSProtocol(websockets.WebSocketServerProtocol):
    async def process_request(self, path, headers):
        print(f"Request, path: {path} ")
        print(f"  Headers: {headers} ")
        # self.cookies = {}
        # # Loop over all Cookie headers
        # for value in headers.get_all("Cookie"):
        #     # split header value by ';' to get each cookie, the split
        #     # cookie by '=' to get name and content of cookie and
        #     # collect these in a dict
        #     self.cookies.update(
        #         {
        #             e[0]: e[1]
        #             for e in [
        #                 v.strip().split("=") for v in value.split(";") if len(v) > 0
        #             ]
        #         }
        #     )
        # # print(f"Cookies: {self.cookies} ")
        # gacho_cookie = json.loads(self.cookies.get("gacho", "{}"))
        # user = gacho_cookie.get("user", None)


class WS_Handler:
    def __init__(self):
        pass

    async def send(self, msg, websocket):
        "Send a merssage to the client"
        await websocket.send(msg)
        print(f"sent message: {msg}")

    async def handler(self, websocket, path):
        "Handle a ws connection"
        print(f"Connection started: {websocket=}")
        try:
            async for message in websocket:
                print(f"Client posted: {message=}")
                # data = json.loads(message)
                await self.send('"Hello User"', websocket)
        finally:
            print("Connection closed.")


async def main():
    handler = WS_Handler()
    async with websockets.serve(handler.handler, "192.168.1.233", 8765):
        print("WS server started.")
        await asyncio.Future()  # run forever
