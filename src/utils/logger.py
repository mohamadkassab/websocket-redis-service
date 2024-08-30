import logging
import os
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime

class Logger:
    def __init__(self, log_dir='logs', log_level=logging.INFO):
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)
        
        # Check if handlers already exist, remove them to avoid duplicates
        if not self.logger.hasHandlers():
            # Create a formatter
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            
            # Handler for console output
            c_handler = logging.StreamHandler()
            c_handler.setLevel(log_level)
            c_handler.setFormatter(formatter)
            self.logger.addHandler(c_handler)
            
            # Handler for file output (rotating daily)
            log_file = os.path.join(log_dir, f'{datetime.now().strftime("%Y-%m-%d")}.log')
            f_handler = TimedRotatingFileHandler(log_file, when='midnight', interval=1)
            f_handler.setLevel(log_level)
            f_handler.setFormatter(formatter)
            self.logger.addHandler(f_handler)
    
    def get_logger(self):
        return self.logger
