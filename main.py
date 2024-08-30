from src.services.webSocketService import WebSocketService
from src.services.redisService import RedisService
import json
import asyncio
import signal
import random
import time
import threading

async def write_data_redis(stop_event):
    redisService = RedisService()
    await redisService.connect()
    await redisService.flushDb()
    while not stop_event.is_set():
        try:
            file_path = r"src\utils\dummyData.json"
            with open(file_path, "r") as json_file:
                data_list = json.load(json_file)
                for data in data_list:
                    value = {
                        "value": data["value"] + random.randint(1, 100),
                    }
                    key = f'{data["sheet_name"]}:{data["sheet_id"]}:{data["manager_login"]}:{data["symbol"]}:{data["type"]}:{data["cov"]}'
                    await redisService.hset(key, value)
                    await redisService.publish('update_channel', [key])
                time.sleep(1)
        except Exception as e:
            pass

async def listen_to_redis(ws):  
    redisService = RedisService()
    await redisService.connect()
    try:
        await redisService.subscribe("update_channel")
        if redisService.ps:
            for message in redisService.ps.listen():
                try:
                    if message and message['type'] == 'message':
                        data = await redisService.hget(f'{message["data"].decode("utf-8")}')
                        message = json.dumps({"type": "updates", "data": data})
                        dataKey = data['key']
                        sheetId = dataKey.split(':')[1]
                        await ws.publish_message(message, sheetId)
                except Exception as e:
                    print(e)
                    
    except Exception as e:
        pass

async def start_websocket_server(ws):
    await ws.start_server()

def run_async_function(async_func, *args):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(async_func(*args))

async def main():
    stop_event = threading.Event()
    ws = WebSocketService()

    def signal_handler(signum, frame):
        stop_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    thread1 = threading.Thread(target=run_async_function, args=(listen_to_redis, ws))
    thread2 = threading.Thread(target=run_async_function, args=(start_websocket_server, ws))
    thread3 = threading.Thread(target=run_async_function, args=(write_data_redis, stop_event))

    thread1.start()
    thread2.start()
    thread3.start()

    stop_event.wait() 

# ENTRY POINT
if __name__ == "__main__":
    asyncio.run(main())

