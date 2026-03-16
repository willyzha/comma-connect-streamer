import os
import threading
import queue
import logging
from os import path
from dataclasses import dataclass
import time
import sys
import pathlib # Added this import
from datetime import datetime # Added this import

# Define a default Segment dataclass for type hinting in the generic ClipsFifo,
# though specific applications can pass their own.
@dataclass
class GenericSegment:
  route_name: str
  segment_num: int
  start_time: int
  end_time: int
  download_url: str

  def unique_name(self):
    return self.route_name + "-" + str(self.segment_num)

class ClipsFifo:
  def __init__(self,
               fifo_path: str,
               loading_clip_path: str,
               offline_clip_bytes_source: bytes | str, # Can be path or actual bytes
               delete_clips: bool = False,
               write_timestamps: bool = False,
               write_text_video_func = None, # Function to call to write text video: func(input_video, output_video, timestamp_str, segment)
               mark_segment_processed_func = None, # Function to call to mark segment processed: func(segment)
               segment_dataclass = GenericSegment # Dataclass type for segment, allows for specific type hinting
              ):
    self.__fifo_path = fifo_path
    self.__loading_clip_path = loading_clip_path
    self.__offline_clip_bytes_source = offline_clip_bytes_source # Path to offline clip or bytes
    self.__delete_clips = delete_clips
    self.__write_timestamps = write_timestamps
    self.__write_text_video_func = write_text_video_func
    self.__mark_segment_processed_func = mark_segment_processed_func
    self.__segment_dataclass = segment_dataclass # Store the dataclass type if needed for internal operations

    self.__run = True
    self.__setup_fifo = queue.Queue(maxsize=10)
    self.__fifo = queue.Queue(maxsize=10)
    self.__callback_fifo = queue.Queue()
    self.__t0 = threading.Thread(target=self.__ProcessSetup, name="FifoSetupThread")
    self.__t1 = threading.Thread(target=self.__ProcessQueue, name="FifoProcessThread")
    self.__t2 = threading.Thread(target=self.__ProcessCallback, name="FifoCallbackThread")
    self.__t0.start()
    self.__t1.start()
    self.__t2.start()

    self.__watchdog = threading.Thread(target=self.__Watchdog, name="FifoWatchdog")
    self.__watchdog.start()

  def AddClip(self, segment, clip_file: str, callback=None):
    # Queue the file path immediately. Processing and reading happens in the background.
    self.__setup_fifo.put((segment, clip_file, callback), timeout=120)

  def __ProcessSetup(self):
    while self.__run:
      try:
        segment, clip_file, callback = self.__setup_fifo.get(timeout=1)
      except queue.Empty:
        continue

      logging.debug(f"__ProcessSetup: processing {clip_file}")

      final_clip_file = clip_file

      # Handle Timestamp/Text writing here (Background Thread)
      if self.__write_timestamps and segment is not None and self.__write_text_video_func is not None:
          try:
            p = pathlib.Path(clip_file)
            # Use clip_file's directory
            text_clip = path.join(path.dirname(clip_file), f"{p.stem}-text{p.suffix}")
            timestamp_str = datetime.fromtimestamp(segment.start_time / 1000).strftime(r'%Y-%m-%d %I\:%M %p')
            
            # This blocking call now happens in the background
            self.__write_text_video_func(clip_file, text_clip, timestamp_str, segment)

            final_clip_file = text_clip

            # Handle deletion of the *original* clip if needed
            if self.__delete_clips:
                try:
                    if os.path.exists(clip_file):
                        os.remove(clip_file)
                except OSError as e:
                    logging.error(f"Error removing original clip {clip_file}: {e}")
          
          except Exception as e:
            logging.error(f"Error applying timestamps to {clip_file}: {e}")
            final_clip_file = clip_file # Fallback to original

      # Read file into RAM (Preserving User Preference)
      clip_bytes = b""
      try:
          with open(final_clip_file, "rb") as fp:
             clip_bytes = fp.read()
      except Exception as e:
          logging.error(f"Error reading clip {final_clip_file}: {e}")
          self.__setup_fifo.task_done()
          continue

      logging.debug(f"__ProcessSetup: len(clip_bytes) {len(clip_bytes)}")

      # If FIFO queue is empty throw in a loading screen first
      if self.__fifo.empty() and self.__loading_clip_path:
        try:
          with open(self.__loading_clip_path, "rb") as fp:
            self.__fifo.put((None, self.__loading_clip_path, fp.read(), None), timeout=120)
        except FileNotFoundError:
          logging.warning(f"Loading clip not found at {self.__loading_clip_path}")
        except Exception as e:
          logging.error(f"Error reading loading clip: {e}")
      
      self.__fifo.put((segment, final_clip_file, clip_bytes, callback), timeout=120)
      self.__setup_fifo.task_done()

  def __ProcessQueue(self):
    fifo_fp = None

    while self.__run:
      # If FIFO is empty, continuously write offline clip
      while self.__fifo.empty() and self.__run:
        try:
          # Handle offline_clip_bytes_source being a path or actual bytes
          if isinstance(self.__offline_clip_bytes_source, str): # It's a path
            with open(self.__offline_clip_bytes_source, "rb") as frame:
              offline_bytes = frame.read()
          else: # It's raw bytes
            offline_bytes = self.__offline_clip_bytes_source

          if fifo_fp is None:
            # Open FIFO only once if possible to avoid overhead
            try:
                fifo_fp = open(self.__fifo_path, "wb")
            except FileNotFoundError:
                os.mkfifo(self.__fifo_path) # Attempt to create if it doesn't exist
                fifo_fp = open(self.__fifo_path, "wb")
            except Exception as e:
                logging.error(f"Error opening FIFO {self.__fifo_path}: {e}")
                time.sleep(5) # Wait before retrying
                continue

          fifo_fp.write(offline_bytes)
          fifo_fp.flush() # Ensure bytes are written immediately
          logging.debug(f"Displaying offline screen to {self.__fifo_path}")
        except FileNotFoundError:
          logging.warning(f"Offline clip source not found: {self.__offline_clip_bytes_source}. Skipping offline clip.")
          time.sleep(5)
        except Exception as e:
          logging.error(f"Error processing offline clip for {self.__fifo_path}: {e}")
          if fifo_fp:
            fifo_fp.close()
            fifo_fp = None
          time.sleep(5) # Wait before retrying
        time.sleep(1) # Prevent busy-waiting

      if not self.__run: # Check run flag again after potentially waiting for fifo.empty()
          break

      try:
        segment, clip_file, clip_bytes, callback = self.__fifo.get(timeout=1)
      except queue.Empty:
        continue # Should not happen often due to outer while loop condition

      if clip_file is not None:
        logging.info(f"Start Processing: {path.basename(clip_file)} for {self.__fifo_path}")

      if fifo_fp is None:
        try:
            fifo_fp = open(self.__fifo_path, "wb")
        except FileNotFoundError:
            os.mkfifo(self.__fifo_path) # Attempt to create if it doesn't exist
            fifo_fp = open(self.__fifo_path, "wb")
        except Exception as e:
            logging.error(f"Error opening FIFO {self.__fifo_path}: {e}")
            self.__fifo.task_done()
            time.sleep(5)
            continue
      
      try:
          fifo_fp.write(clip_bytes)
          fifo_fp.flush()
      except BrokenPipeError:
          logging.warning(f"Broken pipe while writing to {self.__fifo_path}. Reopening FIFO.")
          if fifo_fp:
              fifo_fp.close()
              fifo_fp = None
          # Re-put the current item back to be processed after reopening FIFO
          self.__fifo.put((segment, clip_file, clip_bytes, callback))
          self.__fifo.task_done() # Mark current one as done since we re-queued it
          time.sleep(1) # Small delay to avoid tight loop on broken pipe
          continue
      except Exception as e:
          logging.error(f"Error writing clip bytes to {self.__fifo_path}: {e}")
          if fifo_fp:
              fifo_fp.close()
              fifo_fp = None
          time.sleep(1)
          self.__fifo.task_done()
          continue


      if self.__fifo.empty() and fifo_fp is not None:
        # Close FIFO if queue is empty to allow readers to detect EOF and new readers to open
        fifo_fp.close()
        fifo_fp = None

      if clip_file is not None:
        logging.info(f"Done Processing: {path.basename(clip_file)} for {self.__fifo_path}")
      self.__callback_fifo.put((segment, clip_file, callback))
      self.__fifo.task_done()

  def __ProcessCallback(self):
    while self.__run:
      try:
        segment, clip_file, callback = self.__callback_fifo.get(timeout=1)
      except queue.Empty:
        continue

      if callback is not None:
        try:
          callback()
        except Exception as e:
          logging.error(f"Error in callback for {clip_file}: {e}")

      if segment is not None and self.__mark_segment_processed_func is not None:
        try:
          self.__mark_segment_processed_func(segment)
        except Exception as e:
          logging.error(f"Error marking segment processed for {segment}: {e}")

      # Clean up clip files if delete_clips is True
      if self.__delete_clips and clip_file is not None:
        # Don't delete loading/offline clips if they are file paths
        if clip_file != self.__loading_clip_path and clip_file != self.__offline_clip_bytes_source:
          try:
            if os.path.exists(clip_file):
              os.remove(clip_file)
              logging.debug(f"Deleted clip file: {clip_file}")
          except OSError as e:
            logging.error(f"Error deleting clip file {clip_file}: {e}")
      self.__callback_fifo.task_done()

  def __Watchdog(self):
    while self.__run:
      time.sleep(30)
      if not self.Alive():
        logging.error("ClipsFifo watchdog triggered. One or more threads are not alive.")
        self.Stop()
        break

  def Length(self) -> int:
    return self.__fifo.qsize()

  def Stop(self):
    self.__run = False
    # Signal threads to stop and join them
    logging.info("Stopping ClipsFifo threads...")
    self.__setup_fifo.join() # Wait for all items to be processed
    self.__fifo.join()
    self.__callback_fifo.join()
    
    self.__t0.join(timeout=5)
    self.__t1.join(timeout=5)
    self.__t2.join(timeout=5)
    self.__watchdog.join(timeout=5)

    if self.__t0.is_alive():
        logging.warning("FifoSetupThread did not terminate cleanly.")
    if self.__t1.is_alive():
        logging.warning("FifoProcessThread did not terminate cleanly.")
    if self.__t2.is_alive():
        logging.warning("FifoCallbackThread did not terminate cleanly.")
    if self.__watchdog.is_alive():
        logging.warning("FifoWatchdog did not terminate cleanly.")
    logging.info("ClipsFifo stopped.")

  def Alive(self) -> bool:
    return self.__t0.is_alive() and self.__t1.is_alive() and self.__t2.is_alive()

# Make a dummy CommaDatabase and WriteTextVideo for testing or if not provided
class DummyCommaDatabase:
  def mark_segment_processed(self, segment):
    logging.info(f"DummyCommaDatabase: Marked segment processed: {segment.unique_name()}")

def dummy_write_text_video(input_video: str, output_video: str, timestamp: str, segment):
  logging.info(f"DummyWriteTextVideo: Input: {input_video}, Output: {output_video}, Timestamp: {timestamp}, Segment: {segment.unique_name()}")
  # Simulate creation of a text video by copying the input to output
  try:
      import shutil
      shutil.copyfile(input_video, output_video)
  except FileNotFoundError:
      logging.error(f"DummyWriteTextVideo: Input video not found: {input_video}")
  except Exception as e:
      logging.error(f"DummyWriteTextVideo: Error copying file: {e}")

if __name__ == '__main__':
    # Example usage for testing the generic ClipsFifo
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')

    # Create dummy files for testing
    temp_dir = "/tmp/fifo_test"
    os.makedirs(temp_dir, exist_ok=True)
    test_fifo_path = os.path.join(temp_dir, "test.fifo")
    test_loading_clip = os.path.join(temp_dir, "loading.ts")
    test_offline_clip = os.path.join(temp_dir, "offline.ts")
    test_clip1 = os.path.join(temp_dir, "clip1.ts")
    test_clip2 = os.path.join(temp_dir, "clip2.ts")

    with open(test_loading_clip, "w") as f: f.write("loading content")
    with open(test_offline_clip, "w") as f: f.write("offline content")
    with open(test_clip1, "w") as f: f.write("clip1 content")
    with open(test_clip2, "w") as f: f.write("clip2 content")

    try:
        os.mkfifo(test_fifo_path)
    except FileExistsError:
        pass # Already exists

    # Create an instance of ClipsFifo
    fifo = ClipsFifo(
        fifo_path=test_fifo_path,
        loading_clip_path=test_loading_clip,
        offline_clip_bytes_source=test_offline_clip, # Pass path for offline clip
        delete_clips=True,
        write_timestamps=True,
        write_text_video_func=dummy_write_text_video,
        mark_segment_processed_func=DummyCommaDatabase().mark_segment_processed,
        segment_dataclass=GenericSegment
    )

    # Simulate adding clips
    segment1 = GenericSegment("route_A", 0, 1678886400000, 1678886430000, "url_A")
    segment2 = GenericSegment("route_B", 1, 1678886460000, 1678886490000, "url_B")

    fifo.AddClip(segment1, test_clip1)
    fifo.AddClip(segment2, test_clip2)

    # Simulate a reader for the FIFO
    def fifo_reader():
        logging.info("FIFO Reader: Starting...")
        with open(test_fifo_path, "rb") as f:
            while fifo.Alive():
                content = f.read(1024) # Read in chunks
                if content:
                    logging.info(f"FIFO Reader: Read {len(content)} bytes.")
                else:
                    logging.info("FIFO Reader: No more content. Waiting...")
                time.sleep(0.1)
        logging.info("FIFO Reader: Exiting.")

    reader_thread = threading.Thread(target=fifo_reader, name="FifoReaderThread")
    reader_thread.start()

    # Let it run for a while
    time.sleep(10)

    # Stop the fifo
    fifo.Stop()
    reader_thread.join()

    # Clean up dummy files
    os.remove(test_loading_clip)
    os.remove(test_offline_clip)
    # The actual clips should be deleted by the fifo itself
    os.remove(test_fifo_path)
    os.rmdir(temp_dir)
    logging.info("Test finished and cleaned up.")
