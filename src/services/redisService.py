import redis
from dotenv import load_dotenv
import os
from src.utils.logger import Logger

load_dotenv()

class RedisService:
    def __init__(self):
        self.logger = Logger().get_logger()
        self.client = None
        self.ps = None

    async def connect(self):
        try:
            host = os.getenv('REDIS_HOST')
            port = int(os.getenv('REDIS_PORT'))
            db = int(os.getenv('REDIS_DB'))
            self.client = redis.Redis(host=host, port=port, db=db)
            self.logger.info("Connected to Redis")
        except Exception as e:
            self.logger.error(f"Error connecting to Redis: {e}", exc_info=True)

    async def flushDb(self):
        if self.client:
            self.client.flushdb()
            self.logger.info("Redis DB is being flushed")

    async def disconnect(self):
        if self.client:
            del self.client
            self.client = None
            self.logger.info("Disconnected from Redis")
        
    async def hset(self, key, value):
        if not self.client:
            self.logger.warning("Not connected to Redis")
            return
        try:
            self.client.hset(key, mapping=value)
            # self.logger.info(f"Key '{key}' set to '{value}'")
        except Exception as e:
            self.logger.error(f"Error hmset_key '{key}': {e}", exc_info=True)
            
    async def get_all(self, keyPattern):
        if not self.client:
            self.logger.warning("Not connected to Redis")
            return
        try:
            keys = await self.client.keys(keyPattern)
            dataList = []
            for key in keys:
                value = await self.client.get(key)
                decoded_key = key.decode('utf-8')
                decoded_value = float(value.decode('utf-8')) if value else None
                dataList.append({
                    'key': decoded_key,
                    'data': decoded_value
                })
            return dataList
        except Exception as e:
            self.logger.error(f"Error getting keys matching '{keyPattern}': {e}", exc_info=True)
        return

    async def hget_all(self, keyPattern):
        if not self.client:
            self.logger.warning("Not connected to Redis")
            return
        try:
            keys = self.client.keys(keyPattern)
            dataList = []
            for key in keys:
                value = self.client.hgetall(key)
                decoded_data = {key.decode('utf-8'): value.decode('utf-8') for key, value in value.items()}
                dataList.append({
                   'key': key.decode('utf-8'),
                   'data': decoded_data  
                })
            return dataList
        except Exception as e:
            self.logger.error(f"Error hmget '{key}': {e}", exc_info=True)
            return
        
    async def hget(self, keyPattern):
        if not self.client:
            self.logger.warning("Not connected to Redis")
            return
        try:
            keys = self.client.keys(keyPattern)
            if not keys:
                return None
            
            # Get the first matching key
            first_key = keys[0]
            value = self.client.hgetall(first_key)
            decoded_data = {k.decode('utf-8'): v.decode('utf-8') for k, v in value.items()}
            
            return {
                'key': first_key.decode('utf-8'),
                'data': decoded_data
            }
        except Exception as e:
            self.logger.error(f"Error hget '{keyPattern}': {e}", exc_info=True)
            return
        
    async def set(self, key, value):
        if not self.client:
            self.logger.warning("Not connected to Redis")
            return
        try:
            self.client.set(key, value)
            self.logger.info(f"Key '{key}' set to '{value}'")
        except Exception as e:
            self.logger.error(f"Error setting key '{key}': {e}", exc_info=True)

    async def get(self, key):
        if not self.client:
            self.logger.warning("Not connected to Redis")
            return None
        try:
            value = self.client.get(key)
            if value:
                return value.decode('utf-8')
            else:
                return None
        except Exception as e:
            self.logger.error(f"Error getting key '{key}': {e}", exc_info=True)
            return None
        
    async def publish(self, channel, messages):
        if not self.client:
            self.logger.warning("Not connected to Redis")
            return None
        try:
            for message in messages:
                self.client.publish(channel, message)
        except Exception as e:
            self.logger.error(f"Error publishing key channel '{channel} messages {messages}': {e}", exc_info=True)
            return None
        
    async def subscribe(self, channel):
        if not self.client:
            self.logger.warning("Not connected to Redis")
            return None
        try:
            ps = self.client.pubsub()
            ps.subscribe(channel)
            self.ps = ps
        except Exception as e:
            self.logger.error(f"Error subscribing channel '{channel}': {e}", exc_info=True)
            return None

