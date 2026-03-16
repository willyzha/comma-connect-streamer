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
from dotenv import load_dotenv
from fifo_streamer import ClipsFifo, GenericSegment

# Load configuration from .env file if it exists
script_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(script_dir, '.env')
load_dotenv(env_path)

# Helper to get config with defaults and ENV overrides
def get_config(key, fallback, type=str):
    # Try environment variable first (load_dotenv makes .env vars available as env vars)
    val = os.environ.get(key)
    if val is not None:
        if type == bool:
            return val.lower() in ('true', '1', 't', 'y', 'yes')
        if type == int:
            try:
              return int(val)
            except ValueError:
              return fallback
        return val
    return fallback

# CONFIGS (with ENV/ .env overrides)
DONGLE_ID = get_config('COMMA_DONGLE_ID', 'your_dongle_id_here')
WRITE_TIMESTAMPS = get_config('WRITE_TIMESTAMPS', True, type=bool)
DELETE_CLIPS = get_config('DELETE_CLIPS', True, type=bool)
LOG_LEVEL_STR = get_config('LOG_LEVEL', 'INFO')
LOG_LEVEL = getattr(logging, LOG_LEVEL_STR.upper(), logging.INFO)
CHECK_DATABASE = get_config('CHECK_DATABASE', True, type=bool)
STOP_AT_FIRST_PROCESSED = get_config('STOP_AT_FIRST_PROCESSED', True, type=bool)
END_TIMEDELTA = timedelta(minutes=get_config('END_TIMEDELTA_MINUTES', 5, type=int))
TIME_RANGE = timedelta(days=get_config('TIME_RANGE_DAYS', 3, type=int))
JWT_KEY = get_config('COMMA_JWT_KEY', 'your_jwt_key_here')
if not JWT_KEY.startswith('JWT ') and JWT_KEY != 'your_jwt_key_here':
    JWT_KEY = f"JWT {JWT_KEY}"
HTTP_REQUEST_RETRIES = get_config('HTTP_REQUEST_RETRIES', 10, type=int)
DATABASE_PATH = get_config('DATABASE_PATH', '/config/comma_downloads.db')
FIFO_PATH = get_config('FIFO_PATH', '/dev/shm/new_clip.fifo')
DOWNLOAD_PATH = get_config('DOWNLOAD_PATH', '/dev/shm/dashcam/clips')
FFMPEG_PATH = get_config('FFMPEG_PATH', '/usr/bin/ffmpeg')
FONT_PATH = get_config('FONT_PATH', '/usr/share/fonts/roboto/Roboto-Thin.ttf')
LOADING_PATH = get_config('LOADING_PATH', '/app/loading.ts')
OFFLINE_PATH = get_config('OFFLINE_PATH', '/app/offline.ts')

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
    "-loglevel", "quiet",
    "-nostats",
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

  logging.debug(f"Searching for segments from {start_millis} to {end_millis}...")

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
        logging.debug(f"All {route_name} segments already exist in DB. Skipping.")
        continue
    logging.debug(f"Getting download URLs for route {route_name}")
    download_urls = GetSegmentDownloadUrls(route_name)

    if len(download_urls.keys()) == 0:
      logging.warning(f"Got no URLs for route {route_name} when expected {len(route['segment_numbers'])}. Skipping route.")
      continue
    if len(download_urls.keys()) != len(route['segment_numbers']):
      logging.warning(f"Got an incorrect number of URLs for {route_name}, expected {len(route['segment_numbers'])}, got {len(download_urls)}")
      # return segments

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
  
  logging.info(f"Found {len(segments)} new segments between {start_millis} and {end_millis}.")
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
    logging.debug(f"Found qcamera URL: {download_url}")
    
    match = re.search(r'--(\d+)--qcamera.ts', download_url)
    if match:
      segment_index = int(match.group(1))
      logging.debug(f"Parsed segment index {segment_index} from URL")
      urls[segment_index] = download_url
    else:
      logging.warning(f"Could not parse segment index from URL: {download_url}")

  return urls

def DownloadSegment(segment):
  invalid = '<>:"/\\|?* '
  filename = segment.unique_name()+".ts"
  for char in invalid:
    filename = filename.replace(char, '')

  logging.info(f"Downloading segment: {filename}")
  dir = path.join(DOWNLOAD_PATH, filename)
  urllib.request.urlretrieve(segment.download_url, dir)
  return dir

def main():
  db = None
  try:
    logging.info("Starting comma_download.py")
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
          logging.debug(f"Segment {segment.unique_name()} already exists in DB, skipping.")
          continue
        latest_segment_time = datetime.now(UTC)
        clip = DownloadSegment(segment)
        fifo.AddClip(segment, clip, None)
        db.add_segment(segment)
        if datetime.now(UTC) - get_segment_time > timedelta(minutes=45):
          logging.warning(f"Segment retrieval loop has been running for 45 minutes, breaking to refresh.")
          break

      # If it's been more than 10min since the last new segment reduce the polling freq to 5min
      if datetime.now(UTC) - latest_segment_time > timedelta(minutes=10):
        logging.info("No new segments found in 10 minutes. Lowering polling frequency to 5 minutes.")
        sleep_s = 60 * 5
        db.cleanup()
      else:
        sleep_s = 30

      time.sleep(sleep_s)
      end_time = datetime.now(UTC) - END_TIMEDELTA
      start_time = end_time - TIME_RANGE


  except Exception as e:
    logging.error(f"Critical error in main loop: {e}")
    traceback.print_exc()
  finally:
    logging.info("Shutting down comma_download.py...")
    # Make sure fifo is defined before calling stop in case of early error
    if 'fifo' in locals() and fifo.Alive():
        fifo.Stop()
    else:
        logging.debug("FIFO was not initialized or not alive during shutdown.")
    
    if db:
        db.close()

  return 0

if __name__ == '__main__':
    sys.exit(main())
