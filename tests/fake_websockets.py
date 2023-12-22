import pytest
import asyncio
import websockets

class FakeWebsocketServer():
    def __init__(self, responseMap: dict, listenAddress: str = "127.0.0.1", listenPort: int = 51234):
        self.responseMap = responseMap
        self.listenAddress = listenAddress
        self.listenPort = listenPort

        self.server = None

    async def start(self):
        self.server = await websockets.serve(self.websocketServer, self.listenAddress, self.listenPort)

    async def stop(self, clean: bool = True):
        await self.server.close(close_connections=clean)

    async def websocketServer(self, websocket):
        async for message in websocket:
            try:
                await websocket.send(self.responseMap.get(message))
            except ValueError:
                pass


    async def temp(self):
        async with websockets.serve(responseMapLoop, self.listenAddress, self.listenPort):
            await asyncio.Future()