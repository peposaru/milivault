import logging
import os
import datetime
from datetime import datetime


def initialize_logging():
    """This just initializes the logger to keep track of anything."""
    log_dir = "milivault_logs" # Name of the folder created to hold logs.
    os.makedirs(log_dir, exist_ok=True)
    log_file_name = f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log" # Name of the log file.
    log_file_path = os.path.join(log_dir, log_file_name)

    # Logging Configuration
    logging.basicConfig(
        level=logging.INFO, # Levels are NOTSET , DEBUG , INFO , WARN , ERROR , CRITICAL
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file_path, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    logging.info(f"Logging initialized. Log file: {log_file_path}")