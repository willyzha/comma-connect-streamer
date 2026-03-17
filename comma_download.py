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

from comma_auth import CommaAuth

# Load configuration from .env file if it exists
# We check both the script directory and the current working directory
load_dotenv(os.path.join(os.getcwd(), '.env'))
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))

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

# Initialize Auth
auth = CommaAuth(
    jwt_key=get_config('COMMA_JWT_KEY', None),
    github_user=get_config('GITHUB_USER', None),
    github_pass=get_config('GITHUB_PASS', None),
    cache_path=get_config('JWT_CACHE_PATH', '/data/jwt.cache')
)

HTTP_REQUEST_RETRIES = get_config('HTTP_REQUEST_RETRIES', 10, type=int)
DATABASE_PATH = get_config('DATABASE_PATH', '/config/comma_downloads.db')
FIFO_PATH = get_config('FIFO_PATH', '/dev/shm/new_clip.fifo')
DOWNLOAD_PATH = get_config('DOWNLOAD_PATH', '/dev/shm/dashcam/clips')
FFMPEG_PATH = get_config('FFMPEG_PATH', '/usr/bin/ffmpeg')
FONT_PATH = get_config('FONT_PATH', '/usr/share/fonts/roboto/Roboto-Thin.ttf')
LOADING_PATH = get_config('LOADING_PATH', '/app/loading.ts')
OFFLINE_PATH = get_config('OFFLINE_PATH', '/app/offline.ts')

# Initialize logging globally
logging.basicConfig(
  format='[%(asctime)s] [%(name)s] %(message)s',
  datefmt='%m/%d/%Y %I:%M:%S %p',
  level=LOG_LEVEL,
  handlers=[
    RotatingFileHandler(filename="/app/comma_download.log", maxBytes=1024*1024),
    logging.StreamHandler()])

# Helper to get logger
logger = logging.getLogger('downloader')

epoch = datetime.fromtimestamp(0, timezone.utc)

# Configure a global requests session for connection pooling and retries
api_session = requests.Session()
retries = Retry(total=HTTP_REQUEST_RETRIES,
                backoff_factor=1,
                status_forcelist=[ 500, 502, 503, 504 ])
api_session.mount('https://', HTTPAdapter(max_retries=retries))

def make_api_request(url):
    """Makes an authenticated GET request to the Comma API with robust error handling and auto-refresh."""
    try:
        response = api_session.get(url, headers={'Authorization': auth.token}, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code
        if status_code == 401:
            logger.error("AUTHENTICATION ERROR: Your JWT token is expired or invalid.")
            if auth.handle_401():
                try:
                    # Retry once with the new token
                    response = api_session.get(url, headers={'Authorization': auth.token}, timeout=30)
                    response.raise_for_status()
                    return response.json()
                except Exception as retry_err:
                    logger.error(f"Retry failed after JWT refresh: {retry_err}")
        elif status_code == 403:
            logger.error(f"PERMISSION ERROR: Access forbidden (403). Check if Dongle ID {DONGLE_ID} is correct and accessible with your token.")
        elif status_code == 404:
            logger.error(f"NOT FOUND: The requested resource was not found (404). URL: {url}")
        else:
            logger.error(f"HTTP error {status_code} occurred: {e}")
        raise
    except requests.exceptions.Timeout:
        logger.error(f"TIMEOUT: The request to {url} timed out.")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"NETWORK ERROR: A connection error occurred: {e}")
        raise


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
      self.db.execute("""
        CREATE TABLE comma_clips(url TEXT PRIMARY KEY, date INTEGER, processed INTEGER)
      """)
      self.db.execute("CREATE INDEX IF NOT EXISTS idx_date ON comma_clips(date)")
      self.db.commit()

  def add_segment(self, segment: Segment):
    time_now = unix_time_millis(datetime.now(UTC))
  
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

def GetSegments(start_time=None, end_time=None, check_db=CHECK_DATABASE, db_instance=None):
  start_millis = 0
  if start_time is not None:
    start_millis = unix_time_millis(start_time)

  end_millis = unix_time_millis(datetime.now(UTC))
  if end_time is not None:
    end_millis = unix_time_millis(end_time)

  logger.debug(f"Searching for segments from {start_millis} to {end_millis}...")

  url = f"https://api.commadotai.com/v1/devices/{DONGLE_ID}/routes_segments?start={start_millis}&end={end_millis}"
  
  try:
    routes_data = make_api_request(url)
  except Exception:
    # Error logged by make_api_request
    return []

  segments = []
  db = db_instance if db_instance else CommaDatabase()
  
  try:
    for route in routes_data:
      route_name = route['fullname']
      
      # Find which specific segments in this route are missing from DB
      new_segment_indices = []
      for i in route['segment_numbers']:
        s_check = Segment(route_name, i, -1, -1, "")
        if not db.segment_exists(s_check):
          new_segment_indices.append(i)
      
      if not new_segment_indices:
        logger.debug(f"All segments for route {route_name} already exist in DB. Skipping.")
        continue

      logger.debug(f"Found {len(new_segment_indices)} new segments in route {route_name}. Getting URLs...")
      download_urls = GetSegmentDownloadUrls(route_name)

      for i in new_segment_indices:
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
  finally:
    if not db_instance:
      db.close()
  
  if len(segments) > 0:
    logger.info(f"Found {len(segments)} new segments between {start_millis} and {end_millis}.")
  else:
    logger.debug(f"No new segments found (Poll range: {start_millis} - {end_millis}).")

  return sorted(segments, key=lambda x: x.start_time)

def GetSegmentDownloadUrls(route_fullname):
  url = f"https://api.commadotai.com/v1/route/{route_fullname}/files"
  
  try:
    files_data = make_api_request(url)
  except Exception:
    return {}

  urls = {}
  for download_url in files_data.get('qcameras', []):
    logger.debug(f"Found qcamera URL: {download_url}")
    
    match = re.search(r'--(\d+)--qcamera.ts', download_url)
    if match:
      segment_index = int(match.group(1))
      logger.debug(f"Parsed segment index {segment_index} from URL")
      urls[segment_index] = download_url
    else:
      logger.warning(f"Could not parse segment index from URL: {download_url}")

  return urls

def DownloadSegment(segment):
  invalid = '<>:"/\\|?* '
  filename = segment.unique_name()+".ts"
  for char in invalid:
    filename = filename.replace(char, '')

  logger.info(f"Downloading segment: {filename}")
  dest_path = path.join(DOWNLOAD_PATH, filename)
  
  try:
    # Use the session for downloads to benefit from retries and connection pooling.
    # Comma download URLs are typically pre-signed and don't require the Authorization header,
    # but the session will handle it if it's there (or we could strip it).
    response = api_session.get(segment.download_url, stream=True, timeout=60)
    response.raise_for_status()
    
    with open(dest_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    return dest_path
  except Exception as e:
    logger.error(f"Failed to download segment {filename}: {e}")
    if path.exists(dest_path):
        try:
            os.remove(dest_path)
        except Exception:
            pass
    return None

def main():
  db = None
  try:
    logger.info("Starting comma_download.py")
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
      try:
        get_segment_time = datetime.now(UTC)
        new_segments = GetSegments(start_time, end_time, db_instance=db)
        
        # Process segments in order
        for segment in new_segments:
          # Check again just in case a race condition or crash happened
          if db.segment_exists(segment):
            continue
            
          latest_segment_time = datetime.now(UTC)
          clip = DownloadSegment(segment)
          if clip is None:
              logger.warning(f"Skipping segment {segment.unique_name()} due to download failure.")
              continue
              
          fifo.AddClip(segment, clip, None)
          db.add_segment(segment)
          
          if datetime.now(UTC) - get_segment_time > timedelta(minutes=45):
            logger.warning(f"Processing loop has been running for 45 minutes, breaking to refresh segment list.")
            break

        # If it's been more than 10min since the last new segment reduce the polling freq to 5min
        if datetime.now(UTC) - latest_segment_time > timedelta(minutes=10):
          logger.debug("No new segments found in 10 minutes. Lowering polling frequency to 5 minutes.")
          sleep_s = 60 * 5
          db.cleanup()
        else:
          sleep_s = 30
      except Exception as e:
        logger.error(f"Error in polling loop: {e}")
        # traceback.print_exc() # Keep logging but don't exit
        sleep_s = 30 # Default sleep on error
        
      time.sleep(sleep_s)
      end_time = datetime.now(UTC) - END_TIMEDELTA
      start_time = end_time - TIME_RANGE


  except Exception as e:
    logger.error(f"Critical error in main loop: {e}")
    traceback.print_exc()
  finally:
    logger.info("Shutting down comma_download.py...")
    # Make sure fifo is defined before calling stop in case of early error
    if 'fifo' in locals() and fifo.Alive():
        fifo.Stop()
    else:
        logger.debug("FIFO was not initialized or not alive during shutdown.")
    
    if db:
        db.close()

  return 0

if __name__ == '__main__':
    sys.exit(main())
