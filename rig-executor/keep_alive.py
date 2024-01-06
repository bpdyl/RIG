
from datetime import datetime
from threading import Thread

import pytz
from flask import Flask


app = Flask('')


@app.route('/')
def home():
  tz = pytz.timezone('Asia/Kathmandu')
  np_time = datetime.now(tz)
  print("--------------------------------")
  # logger.info(np_time)
  print(np_time)
  print("--------------------------------")
  return "RIG is running"


def run():
  app.run(host='0.0.0.0', port=8080)


def keep_alive():
  t = Thread(target=run)
  t.start()
