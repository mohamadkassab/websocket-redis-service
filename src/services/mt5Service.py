from dotenv import load_dotenv
from src.utils.logger import Logger
import os
import MT5Manager

load_dotenv()

class PositionSink:
    def OnPositionAdd(self, position):
        print(f"Position added: {position.Print()}")

    def OnPositionUpdate(self, position):
        print(f"Position updated: {position.Print()}")

class MT5Service:
    def __init__(self):
        loggerInstance = Logger()
        self.logger = loggerInstance.get_logger()
        self.mt5Ip = f"{os.getenv('MT5_IP')}:{os.getenv('MT5_PORT')}"
        self.manager = MT5Manager.ManagerAPI()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.disconnect()

    async def connect(self, login, password):
        try:
            self.manager = MT5Manager.ManagerAPI()
            if self.manager.Connect(self.mt5Ip, login, password, MT5Manager.ManagerAPI.EnPumpModes.PUMP_MODE_USERS, 120000):
                self.logger.info(f"Connected to MT5 Server by manager {login}")
            else:
                self.logger.warning(f"Failed to connect to MT5 Server: {MT5Manager.LastError()}")
        except Exception as e:
            self.logger.error(f"Error connecting to MT5 Server : {e}", exc_info=True)

    def disconnect(self):
        try:
            if self.manager.Disconnect():
                self.logger.info("Disconnected from MT5 Server")
            else:
                self.logger.error("Failed to disconnect from MT5 Server")
        except Exception as e:
            self.logger.error(f"Exception during disconnection : {e}", exc_info=True)

    async def subsrcibe_position(self):
        try:
          sink = PositionSink()
          if not self.manager.PositionSubscribe(sink):
                self.logger.warning(f"Failed to subscribe: {MT5Manager.LastError()}")
          else:
                self.logger.info("Subscribed to positions")

        except Exception as e:
            self.logger.error(f"Exception during subsrcibe_position : {e}", exc_info=True)





