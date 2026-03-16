import traceback
import requests
from requests.adapters import HTTPAdapter, Retry
import json
from datetime import datetime,timedelta,timezone,UTC
import sqlite3
import re
import urllib.request
from os import path
import pathlib
import os
import time
import subprocess
import threading
import queue
import logging
from logging.handlers import RotatingFileHandler
from dataclasses import dataclass 
import sys
import shutil
import configparser
from fifo_streamer import ClipsFifo, GenericSegment

# Load configuration
config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(script_dir, 'config.ini')
config.read(config_path)

# CONFIGS from config.ini
DONGLE_ID = config.get('COMMA', 'DONGLE_ID')
WRITE_TIMESTAMPS = config.get('COMMA', 'DONGLE_ID')
WRITE_TIMESTAMPS = config.getboolean('COMMA', 'WRITE_TIMESTAMPS')
DELETE_CLIPS = config.getboolean('COMMA', 'DELETE_CLIPS')
LOG_LEVEL = getattr(logging, config.get('COMMA', 'LOG_LEVEL'))
CHECK_DATABASE = config.getboolean('COMMA', 'CHECK_DATABASE')
STOP_AT_FIRST_PROCESSED = config.getboolean('COMMA', 'STOP_AT_FIRST_PROCESSED')
END_TIMEDELTA = timedelta(minutes=config.getint('COMMA', 'END_TIMEDELTA_MINUTES'))
TIME_RANGE = timedelta(days=config.getint('COMMA', 'TIME_RANGE_DAYS'))
JWT_KEY = config.get('COMMA', 'JWT_KEY')
HTTP_REQUEST_RETRIES = config.getint('COMMA', 'HTTP_REQUEST_RETRIES')
DATABASE_PATH = config.get('COMMA', 'DATABASE_PATH')
FIFO_PATH = config.get('COMMA', 'FIFO_PATH')
DOWNLOAD_PATH = config.get('COMMA', 'DOWNLOAD_PATH')
FFMPEG_PATH = config.get('COMMON', 'FFMPEG_PATH')
FONT_PATH = config.get('COMMA', 'FONT_PATH')
LOADING_PATH = config.get('COMMA', 'LOADING_PATH')
OFFLINE_PATH = config.get('COMMA', 'OFFLINE_PATH')

epoch = datetime.fromtimestamp(0, timezone.utc)
logging.basicConfig(
  format='[%(asctime)s] [COMMA] %(message)s',
  datefmt='%m/%d/%Y %I:%M:%S %p',
  level=LOG_LEVEL,
  handlers=[
    RotatingFileHandler(filename="/app/comma_download.log", maxBytes=1024*1024),
    logging.StreamHandler()])


@dataclass
class Segment:
  route_name: str
  segment_num: int
  start_time: int
  end_time: int
  download_url: str

  def unique_name(self):
    return self.route_name + "-" + str(self.segment_num)


def WriteTextVideo(input_video: str, output_video: str, timestamp: str, segment: Segment):

  segment_str = segment.unique_name().replace('|','-')
  drawtext_timestamp = f"drawtext=fontfile={FONT_PATH}:text='{timestamp}':fontcolor=white:fontsize=16:box=1:boxcolor=black@0.2:boxborderw=5:x=(w-text_w)-3:y=(h-text_h)-5"
  drawtext_segment = f"drawtext=fontfile={FONT_PATH}:text='{segment_str}':fontcolor=white:fontsize=16:box=1:boxcolor=black@0.2:boxborderw=5:x=3:y=(h-text_h)-5"

  cmd = [
    FFMPEG_PATH,
    "-y",
    "-i",
    input_video, 
    "-vf",
    f"[in]{drawtext_timestamp},{drawtext_segment}[out]",
    "-vcodec", "mpeg2video",
    "-b:v", "11000k",
    "-maxrate", "11000k",
    "-minrate", "11000k",
    "-r", "50",
    "-top", "1",
#    "-crf", "18",
#    "-qscale:v", "0",
    "-acodec", "copy",
    "-pix_fmt", "yuv422p",
    "-vtag", "xd5c",
    output_video]
  subprocess.call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
#  subprocess.call(cmd)
  time.sleep(5)

class CommaDatabase:
  def __init__(self):
    self.db = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    self.lock = threading.RLock()

  def close(self):
    self.db.close()

  def __get_static_url(self, url):
    return re.sub(r'\?.+', '', url)

  def exists(self):
    with self.lock:
      res = self.db.execute("""
        SELECT name FROM sqlite_master WHERE type='table' AND name='comma_clips';
  """)
      exists = res.fetchone() is not None
      return exists

  def create(self):
    with self.lock:
      if self.exists():
        return
      res = self.db.execute("""
        CREATE TABLE comma_clips(url, date, processed)
  """)

  def add_segment(self, segment: Segment):
    time = unix_time_millis(datetime.now(UTC))
  
    rows = [
      (segment.unique_name(), segment.start_time, 0)
    ]
    with self.lock:
      self.db.executemany(
        "INSERT INTO comma_clips VALUES(?,?,?)", rows)
      self.db.commit()

  def mark_segment_processed(self, segment: Segment):
    params = (1,segment.unique_name(),)
    with self.lock:
      self.db.execute("UPDATE comma_clips SET processed = ? WHERE url = ?", params)
      self.db.commit()

  def segment_exists(self, segment: Segment):
    params = (segment.unique_name(),)
    with self.lock:
      res = self.db.execute("SELECT * FROM comma_clips WHERE url = ?", params)
      exists = res.fetchone() is not None
      return exists

  def cleanup_unprocessed(self):
    with self.lock:
      res = self.db.execute("DELETE FROM comma_clips WHERE processed = 0")
      self.db.commit()

  def cleanup(self):
    cleanup_date = datetime.now(UTC) - timedelta(days=4)
    params = (unix_time_millis(cleanup_date),)
    with self.lock:
      res = self.db.execute("DELETE FROM comma_clips WHERE date < ?", params)
      self.db.commit()

  def print(self):
    with self.lock:
      for row in self.db.execute("SELECT * FROM comma_clips ORDER BY date"):
        print(row)



def unix_time_millis(dt):
  return round((dt - epoch).total_seconds() * 1000.0)

def GetAvailableSegments():

  s = requests.Session()
  retries = Retry(total=HTTP_REQUEST_RETRIES,
                  backoff_factor=1,
                  status_forcelist=[ 500, 502, 503, 504 ])
  s.mount('https://', HTTPAdapter(max_retries=retries))

  millis = unix_time_millis(datetime.now())
  url = f"https://api.commadotai.com/v1/devices/{DONGLE_ID}/routes_segments?start=0&end={millis}"
  response = s.get(url, headers={'Authorization': JWT_KEY})

  fullnames = []
  for route_segment in response.json():
    fullnames.append(route_segment['fullname'])
  return fullnames

def GetSegments(start_time=None, end_time=None, check_db=CHECK_DATABASE, db_instance=None):
  start_millis = 0
  if start_time is not None:
    start_millis = unix_time_millis(start_time)

  end_millis = unix_time_millis(datetime.now(UTC))
  if end_time is not None:
    end_millis = unix_time_millis(end_time)

  logging.info(f"Get segments from {start_millis} to {end_millis}")

  s = requests.Session()
  retries = Retry(total=HTTP_REQUEST_RETRIES,
                  backoff_factor=1,
                  status_forcelist=[ 500, 502, 503, 504 ])
  s.mount('https://', HTTPAdapter(max_retries=retries))

  url = f"https://api.commadotai.com/v1/devices/{DONGLE_ID}/routes_segments?start={start_millis}&end={end_millis}"
  response = s.get(url, headers={'Authorization': JWT_KEY})

  segments = []
  for route in response.json():
    route_name = route['fullname']

    if check_db:
      # Use provided db_instance or create a temporary one (not recommended for performance)
      db = db_instance if db_instance else CommaDatabase()
      all_segments_exist = True
      for i in route['segment_numbers']:
        s = Segment(route_name, i, -1, -1, "")
        if not db.segment_exists(s):
          all_segments_exist = False
      
      if not db_instance: # Close if we created a temp one
          db.close()

      if all_segments_exist:
        logging.debug(f"All {route_name} segments exist in database")
        continue
    logging.debug(f"Getting segments for route {route_name}")
    download_urls = GetSegmentDownloadUrls(route_name)

    if len(download_urls.keys()) == 0:
      logging.warning(f"Got no URLs when expected {len(route['segment_numbers'])}. Skipping route {route_name}")
      continue
    if len(download_urls.keys()) != len(route['segment_numbers']):
      logging.warning(f"Got an incorrect number of URLs, expected {len(route['segment_numbers'])}, got {len(download_urls)}")
      # return segments

    logging.debug(f"Route: {route}")

    for i in route['segment_numbers']:
      if i in download_urls.keys():
        segment_start_time = route['segment_start_times'][i]
        segment_end_time = route['segment_end_times'][i]

        if segment_end_time > end_millis:
          continue

        segments.append(
          Segment(route_name,
                  i,
                  segment_start_time,
                  segment_end_time,
                  download_urls[i]))
  return sorted(segments, key=lambda x: x.start_time)

def GetSegmentDownloadUrls(route_fullname):
  s = requests.Session()
  retries = Retry(total=HTTP_REQUEST_RETRIES,
                  backoff_factor=1,
                  status_forcelist=[ 500, 502, 503, 504 ])
  s.mount('https://', HTTPAdapter(max_retries=retries))

  url = f"https://api.commadotai.com/v1/route/{route_fullname}/files"
  response = s.get(url, headers={'Authorization': JWT_KEY})

  urls = {}
  for download_url in response.json()['qcameras']:
    logging.debug(f"{download_url}")
    
    match = re.search(r'--(\d+)--qcamera.ts', download_url)
    logging.debug(match.group())
    urls[int(match.group(1))] = download_url

  return urls

def DownloadSegment(segment):
  invalid = '<>:"/\\|?* '
  filename = segment.unique_name()+".ts"
  for char in invalid:
    filename = filename.replace(char, '')

  logging.debug("Downloading segment: " + filename)
  dir = path.join(DOWNLOAD_PATH, filename)
  urllib.request.urlretrieve(segment.download_url, dir)
  return dir

def main():
  db = None
  try:
    logging.info("Start comma_download.py")
    db = CommaDatabase()
    db.create()
    db.cleanup_unprocessed()
    fifo = ClipsFifo(
        fifo_path=FIFO_PATH,
        loading_clip_path=LOADING_PATH,
        offline_clip_bytes_source=OFFLINE_PATH,
        delete_clips=DELETE_CLIPS,
        write_timestamps=WRITE_TIMESTAMPS,
        write_text_video_func=WriteTextVideo,
        mark_segment_processed_func=db.mark_segment_processed,
        segment_dataclass=Segment
    )

    end_time = datetime.now(UTC) - END_TIMEDELTA
    start_time = end_time - TIME_RANGE
    latest_segment_time = datetime.now(UTC)
    sleep_s = 30

    while fifo.Alive():
      #segments = GetSegments(start_time, end_time)
      
      unprocessed_segments = queue.LifoQueue()

      get_segment_time = datetime.now(UTC)
      for segment in reversed(GetSegments(start_time, end_time, db_instance=db)):
        if STOP_AT_FIRST_PROCESSED and db.segment_exists(segment):
          break
        unprocessed_segments.put(segment)

      #for segment in iter(unprocessed_segments.get):
      while not unprocessed_segments.empty():
        segment = unprocessed_segments.get()
        if db.segment_exists(segment):
          logging.debug(f"{segment} already exists")
          continue
        latest_segment_time = datetime.now(UTC)
        clip = DownloadSegment(segment)
        fifo.AddClip(segment, clip, None)
        db.add_segment(segment)
        if datetime.now(UTC) - get_segment_time > timedelta(minutes=45):
          logging.debug(f"Segments haven't refreshed in 45min")
          break

      # If it's been more than 10min since the last new segment reduce the polling freq to 5min
      if datetime.now(UTC) - latest_segment_time > timedelta(minutes=10):
        logging.debug("Lowering polling requencey to 5min")
        sleep_s = 60 * 5
        db.cleanup()
      else:
        sleep_s = 30

      time.sleep(sleep_s)
      end_time = datetime.now(UTC) - END_TIMEDELTA
      start_time = end_time - TIME_RANGE


  except Exception as e:
    logging.error(f"Something died: {e}")
    traceback.print_exc()
  finally:
    logging.error(f"stopping the fifo before exiting")
    # Make sure fifo is defined before calling stop in case of early error
    if 'fifo' in locals() and fifo.Alive():
        fifo.Stop()
    else:
        logging.error("Fifo was not initialized or not alive, cannot stop.")
    
    if db:
        db.close()

  return 0

if __name__ == '__main__':
    sys.exit(main())
