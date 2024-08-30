import json
import jwt
import websockets
from src.services.redisService import RedisService
import asyncio
class WebSocketService:
    def __init__(self, host='127.0.0.1', port=8765):
        self.host = host
        self.port = port
        self.clients = {}
        self.redisService = RedisService()
    
    async def start_server(self):
        try:
            await self.redisService.connect()
            server = await websockets.serve(self.handle_client, self.host, self.port)
            print(f"WebSocket server started at ws://{self.host}:{self.port}")
            await server.wait_closed()
        except Exception as e:
            print(f"Error starting WebSocket server: {e}")

    async def handle_client(self, websocket, path):
        client_id = id(websocket)
        self.clients[client_id] = {
            'websocket': websocket,
            'user_id': None,
            'subscribed_sheet_id': None  
        }
        print(f"Client connected: {client_id}")

        try:
            async for message in websocket:
                await self.handle_message(client_id, message)
        except websockets.exceptions.ConnectionClosedError:
            pass
        except Exception as e:
            print(f"Error handling client {client_id}: {e}")
        finally:
            del self.clients[client_id]
            print(f"Client disconnected: {client_id}")

    async def handle_message(self, client_id, message):
        try:
            message_dict = json.loads(message)
            if message_dict.get("type") == "auth":
                user_id = await self.decode_auth(message_dict)
                if user_id:
                    self.clients[client_id]['user_id'] = user_id
                else:
                    await self.send_message(client_id, "auth_failed", {})
                    del self.clients[client_id]

            elif message_dict.get("type") == "subscribe":
                subscribed_sheet_id = message_dict.get("stringSelectedSheetId")
                if subscribed_sheet_id:
                    self.clients[client_id]['subscribed_sheet_id'] = subscribed_sheet_id
                    await self.prepare_initial_data(client_id)
                    print(f"Client {client_id} subscribed to sheet {subscribed_sheet_id}")

        except json.JSONDecodeError:
            print("Invalid message format")
        except Exception as e:
            print(f"Error processing message from client {client_id}: {e}")

    async def prepare_initial_data(self, client_id):
        try:
            subscribed_sheet_id = self.clients[client_id]['subscribed_sheet_id']
            if subscribed_sheet_id:
                data = await self.redisService.hget_all(f'*:{subscribed_sheet_id}:*')
                print(data)
                await self.send_message(client_id, "initial_data", data)
        except Exception as e:
            print(f"Error preparing initial data for client {client_id}: {e}")

    async def send_message(self, client_id, messageType, data):
        try:
            message = json.dumps({"type": messageType, "data": data})
            websocket = self.clients[client_id]['websocket']
            await websocket.send(message)
        except KeyError:
            print(f"Client {client_id} not found or disconnected")
        except websockets.exceptions.ConnectionClosedError:
            print(f"Connection closed while sending message to client {client_id}")
        except Exception as e:
            print(f"Error sending message to client {client_id}: {e}")

    async def publish_message(self, message, sheet_id):
        tasks = []
        try:
            for client_id, client_info in self.clients.items():
                if client_info['subscribed_sheet_id'] == sheet_id:
                    websocket = client_info['websocket']
                    if websocket.open:
                        tasks.append(websocket.send(message))
                    else:
                        print(f"WebSocket for client {client_id} is closed.")
            
            if tasks:
                await asyncio.gather(*tasks)
        
        except websockets.exceptions.ConnectionClosedError as e:
            print(f"ConnectionClosedError: {e}")
        except Exception as e:
            print(f"Error publishing message to clients: {e}")

    async def decode_auth(self, message):
        try:
            auth_token = message.get("authorizationToken")
            if auth_token:
                splitted_token = auth_token.split()[1]
                decoded = jwt.decode(splitted_token, options={"verify_signature": False})
                client_id = decoded.get('id')
                return client_id
            else:
                return None
        except Exception as e:
            print(f"Error decoding authentication token: {e}")

