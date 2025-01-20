import logging
import os
import datetime
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
import subprocess

def initialize_logging():
    """This just initializes the logger to keep track of anything."""
    log_dir = "milivault_logs" # Name of the folder created to hold logs.
    os.makedirs(log_dir, exist_ok=True)
    log_file_name = f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log" # Name of the log file.
    log_file_path = os.path.join(log_dir, log_file_name)

    # Logging Configuration
    logging.basicConfig(
        level=logging.DEBUG, # Levels are NOTSET , DEBUG , INFO , WARN , ERROR , CRITICAL
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file_path, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    logging.debug(f"Logging initialized. Log file: {log_file_path}")




# class WatchdogHandler(FileSystemEventHandler):
#     def on_modified(self, event):
#         # Restart the program if a crash is detected
#         if "crash.log" in event.src_path:
#             logging.info("Crash detected. Restarting program...")
#             subprocess.Popen(["python", "/home/ec2-user/projects/AWS-Militaria-Scraper/AWS_MILITARIA_SCRAPER_JSON.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

# if __name__ == "__main__":
#     logging.basicConfig(filename="scraper.log", level=logging.INFO)
#     event_handler = WatchdogHandler()
#     observer = Observer()
#     observer.schedule(event_handler, path=".", recursive=False)
#     observer.start()

#     try:
#         while True:
#             time.sleep(1)
#     except KeyboardInterrupt:
#         observer.stop()
#     observer.join()