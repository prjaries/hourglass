## ---------------------------
##
## Script name: hourglass.py
##
## Purpose of script: Schedules content on the Project Aries STAR feeds for playback.
##
## Author: physicsprop
##
## Date Created: 07-27-2025
##
## ---------------------------
##
## Notes: requires CasparCG Server as well as ffmpeg in your PATH.
##
## ---------------------------


import time
import json
import socket
import random
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from collections import deque

BASE_DIR = Path(__file__).resolve().parent
EPISODES_FOLDER = BASE_DIR / "episodes"
FILLER_FOLDER = BASE_DIR / "filler"
SLOT_VIDEO = BASE_DIR / "slot_clip.ts"
SLOT_DURATION = 65
SLOT_MINUTE = 18
COMMERCIAL_PADDING = 120
PLAYER_SLEEP = 1
CASPAR_HOST = "localhost"
CASPAR_PORT = 5250
SLOT_TS_FOLDER = BASE_DIR / "slot_clips_ts"
LOG_FILE = BASE_DIR / "playout_log.json"
VIDEO_EXTENSIONS = {".mp4", ".mkv", ".mov", ".ts", ".avi"}
QUEUE_MAX_SIZE = 5

recent_fillers = deque(maxlen=5)
play_queue = []

current_show_index = 0
episodes_played_from_show = 0
EPISODES_PER_SHOW = 1

def normalize_path(path):
    try:
        if isinstance(path, str):
            path = Path(path)
        return str(path.resolve()).replace("\\", "/")
    except Exception as e:
        print(f"[ERROR] Failed to normalize path: {e}")
        return str(path)

def get_show_folders():
    try:
        return [f for f in EPISODES_FOLDER.iterdir() if f.is_dir()]
    except Exception as e:
        print(f"[ERROR] Failed to list show folders: {e}")
        return []
show_folders = get_show_folders()

class CasparCGClient:
    def __init__(self, host=CASPAR_HOST, port=CASPAR_PORT):
        self.host = host
        self.port = port

    def send_command(self, cmd):
        try:
            with socket.create_connection((self.host, self.port), timeout=2) as sock:
                sock.sendall((cmd + "\r\n").encode())
                return sock.recv(1024).decode().strip()
        except Exception as e:
            print(f"[ERROR] Failed to send command '{cmd}': {e}")
            return None

    def play_video(self, path, channel=1, layer=10,
                   audio_channels=None, audio_map=None):
        cmd = f'PLAY {channel}-{layer} "{normalize_path(path)}"'
        if audio_channels:
            cmd += f" --audioChannels {audio_channels}"
        if audio_map:
            cmd += f" --audioMap {audio_map}"
        return self.send_command(cmd)

    def overlay_caption(self, text, channel=1, layer=20, template="timestamp_template"):
        json_data = f'{{"text":"{text}"}}'
        return self.send_command(f'CG ADD {channel}-{layer} 0 "{template}" 1 "{json_data}"')

caspar = CasparCGClient()

def get_random_slot_ts():
    try:
        slot_files = sorted(SLOT_TS_FOLDER.glob("*.ts"))
        if not slot_files:
            raise FileNotFoundError("No .ts files found in slot folder.")
        return random.choice(slot_files)
    except Exception as e:
        print(f"[ERROR] Slot TS selection failed: {e}")
        return SLOT_VIDEO

def time_until_next_slot():
    try:
        now = datetime.now()
        next_slot = now.replace(minute=SLOT_MINUTE, second=0, microsecond=0)
        if now.minute > SLOT_MINUTE or (now.minute == SLOT_MINUTE and now.second > 0):
            next_slot += timedelta(hours=1)
        return max(0, (next_slot - now).total_seconds())
    except Exception as e:
        print(f"[ERROR] Failed to calculate time until slot: {e}")
        return SLOT_DURATION

duration_cache = {}

def get_video_duration(file_path):
    #print(f"[DEBUG] Probing duration for: {file_path}")
    file_path = str(file_path)
    if file_path in duration_cache:
        return duration_cache[file_path]

    try:
        result = subprocess.run([
            "ffprobe", "-v", "error", "-show_entries",
            "format=duration", "-of", "json", file_path
        ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=5)
        data = json.loads(result.stdout)
        duration = float(data["format"]["duration"])
        duration_cache[file_path] = duration
        return duration
    except Exception as e:
        print(f"[ERROR] Could not get duration for '{file_path}': {e}")
        duration_cache[file_path] = 0
        return 0

        
def play_video_with_caption(video_path, duration, label):
    try:
        ts = int(time.time())
        #caspar.overlay_caption(f"{label} - Timestamp: {ts}")
        caspar.play_video(video_path)
        time.sleep(PLAYER_SLEEP)
    except Exception as e:
        print(f"[ERROR] Playback failed for '{video_path}': {e}")

def get_next_random_episode():
    global current_show_index, episodes_played_from_show

    if not show_folders:
        print("[WARN] No show folders found. Falling back to filler.")
        return None

    current_show = show_folders[current_show_index]
    episode_candidates = [
        f for f in current_show.iterdir()
        if f.is_file() and f.suffix.lower() in VIDEO_EXTENSIONS and f.name != SLOT_VIDEO.name
    ]

    if not episode_candidates:
        print(f"[WARN] No episodes found in {current_show.name}. Rotating.")
        current_show_index = (current_show_index + 1) % len(show_folders)
        episodes_played_from_show = 0
        return get_next_random_episode()

    selected = random.choice(episode_candidates)
    episodes_played_from_show += 1

    if episodes_played_from_show >= EPISODES_PER_SHOW:
        current_show_index = (current_show_index + 1) % len(show_folders)
        episodes_played_from_show = 0

    return selected


def refill_queue():
    while len(play_queue) < QUEUE_MAX_SIZE:
        next_clip = get_next_random_episode()
        if not next_clip or not next_clip.exists():
            continue
        duration = get_video_duration(next_clip)
        if duration <= 1:
            continue
        play_queue.append({
            "path": normalize_path(next_clip),
            "type": "EPISODE" if next_clip.parent == EPISODES_FOLDER else "FILLER",
            "label": next_clip.name,
            "duration": duration
        })

def get_random_filler():
    fillers = [
        f for f in FILLER_FOLDER.iterdir()
        if f.is_file() and f.suffix.lower() in VIDEO_EXTENSIONS and f not in recent_fillers
    ]
    if not fillers:
        recent_fillers.clear()
        fillers = [
            f for f in FILLER_FOLDER.iterdir()
            if f.is_file() and f.suffix.lower() in VIDEO_EXTENSIONS
        ]
    return random.choice(fillers) if fillers else None

def play_filler_until_slot(seconds_remaining):
    try:
        while seconds_remaining > SLOT_DURATION:
            filler = get_random_filler()
            if not filler or not filler.exists():
                print("[WARN] No valid filler found.")
                break
            duration = get_video_duration(filler)
            if duration <= 1 or duration >= seconds_remaining - 2:
                continue
            print(f"[INFO] Playing filler: {filler.name} ({duration:.1f}s)")
            play_video_with_caption(filler, duration, "FILLER")
            recent_fillers.append(filler)
            time.sleep(duration)
            seconds_remaining -= duration
    except Exception as e:
        print(f"[ERROR] Filler playback failed: {e}")

def play_commercial_block(duration):
    print(f"[INFO] Playing commercial padding block (~{duration} sec)")
    play_filler_until_slot(duration)

def get_fitting_episode(max_duration):
    show_dirs = [d for d in EPISODES_FOLDER.iterdir() if d.is_dir()]
    random.shuffle(show_dirs)

    best_single = None
    smallest_gap = max_duration

    # Try to find best single episode
    for show in show_dirs:
        episodes = [
            f for f in show.iterdir()
            if f.is_file() and f.suffix.lower() in VIDEO_EXTENSIONS and f.name != SLOT_VIDEO.name
        ]
        random.shuffle(episodes)
        for ep in episodes:
            duration = get_video_duration(ep)
            if 1 < duration <= max_duration:
                gap = max_duration - duration
                if gap < smallest_gap:
                    best_single = [{
                        "path": normalize_path(ep),
                        "type": "EPISODE",
                        "label": f"{show.name} - {ep.name}",
                        "duration": duration
                    }]
                    smallest_gap = gap

    # Try to find best pair of episodes
    all_eps = [
        f for d in show_dirs for f in d.iterdir()
        if f.is_file() and f.suffix.lower() in VIDEO_EXTENSIONS and f.name != SLOT_VIDEO.name
    ]
    random.shuffle(all_eps)

    best_pair = None
    smallest_pair_gap = max_duration

    for i in range(len(all_eps)):
        for j in range(i + 1, len(all_eps)):
            d1 = get_video_duration(all_eps[i])
            d2 = get_video_duration(all_eps[j])
            total = d1 + d2
            if 2 < total <= max_duration:
                gap = max_duration - total
                if gap < smallest_pair_gap:
                    best_pair = [
                        {
                            "path": normalize_path(all_eps[i]),
                            "type": "EPISODE",
                            "label": all_eps[i].name,
                            "duration": d1
                        },
                        {
                            "path": normalize_path(all_eps[j]),
                            "type": "EPISODE",
                            "label": all_eps[j].name,
                            "duration": d2
                        }
                    ]
                    smallest_pair_gap = gap

    # If no pair or single episode fits, return a filler block
    if not best_pair and not best_single:
        filler = get_random_filler()
        if filler and filler.exists():
            duration = get_video_duration(filler)
            return [{
                "path": normalize_path(filler),
                "type": "FILLER",
                "label": f"FALLBACK - {filler.name}",
                "duration": duration
            }]
        else:
            print("[WARN] No fallback filler available.")
            return None

    return best_pair if best_pair else best_single


def scheduler():
    def should_play_slot(now, last_hour):
        remaining = time_until_next_slot()
        early_window = remaining + 60
        late_window = remaining - 60

        # Check if slot hasn't played this hour
        if now.hour != last_hour:
            # If filler or episode ends within ±60s of slot, trigger it
            if 0 <= remaining <= 60 or -60 <= remaining <= 0:
                print(f"[INFO] Flexible slot launch: {remaining:.2f}s offset")
                slot_clip = get_random_slot_ts()
                play_video_with_caption(slot_clip, SLOT_DURATION, "SLOT")
                time.sleep(SLOT_DURATION)
                return now.hour
        else:
            print("[SKIP] Slot already played this hour.")
        return last_hour

    last_slot_hour = None
    print(f"[STARTUP] Scheduler booting at {datetime.now()}")

    startup_time = time.time()
    refill_queue()
    if time.time() - startup_time > 10:
        print("[ERROR] refill_queue took too long — potential hang.")

    while True:
        try:
            if not EPISODES_FOLDER.exists():
                print("[WARN] Episodes folder missing. Waiting...")
                time.sleep(30)
                continue

            seconds_to_slot = time_until_next_slot()
            max_episode_duration = seconds_to_slot - SLOT_DURATION - 2

            # 🎯 Try fitting optimal episodes
            fitting_episodes = get_fitting_episode(max_episode_duration)

            if isinstance(fitting_episodes, list):
                for item in fitting_episodes:
                    if not Path(item["path"]).exists():
                        print(f"[WARN] Skipping missing file: {item['label']}")
                        continue
                    print(f"[INFO] Playing fitting: {item['label']} ({item['duration']:.1f}s)")
                    play_video_with_caption(item["path"], item["duration"], item["type"])
                    time.sleep(max(0, item["duration"] - 2))

                # Check slot timing after fitting episodes
                last_slot_hour = should_play_slot(datetime.now(), last_slot_hour)
                refill_queue()
                continue

            # Fallback to queue
            print("[INFO] No fitting episode found. Using next in queue.")
            if not play_queue:
                refill_queue()
            current_item = play_queue.pop(0) if play_queue else None

            if not current_item:
                print("[WARN] No item to play. Waiting briefly...")
                time.sleep(5)
                continue

            if not Path(current_item["path"]).exists():
                print("[WARN] Skipping missing file.")
                continue

            if current_item["duration"] >= seconds_to_slot - SLOT_DURATION:
                print("[INFO] Slot too close. Playing fillers.")
                play_filler_until_slot(seconds_to_slot - SLOT_DURATION)
                last_slot_hour = should_play_slot(datetime.now(), last_slot_hour)
                refill_queue()
                continue

            print(f"[INFO] Playing queue: {current_item['label']} ({current_item['duration']:.1f}s)")
            play_video_with_caption(current_item["path"], current_item["duration"], current_item["type"])
            time.sleep(max(0, current_item["duration"] - 2))

            remaining = time_until_next_slot()
            if play_queue:
                next_item = play_queue[0]
                if next_item["duration"] < remaining - SLOT_DURATION:
                    print(f"[INFO] Playing next in queue: {next_item['label']}")
                    play_video_with_caption(next_item["path"], next_item["duration"], next_item["type"])
                    time.sleep(next_item["duration"])
                    play_queue.pop(0)
                    refill_queue()
                else:
                    print("[INFO] Slot too close after current. Skipping queued item.")

            # Filler + commercial padding
            remaining = time_until_next_slot()
            if remaining > SLOT_DURATION + COMMERCIAL_PADDING:
                play_commercial_block(COMMERCIAL_PADDING)
                remaining = time_until_next_slot()

            if remaining > SLOT_DURATION:
                print(f"[INFO] Playing filler until slot ({remaining:.1f}s remaining)")
                play_filler_until_slot(remaining - SLOT_DURATION)
                last_slot_hour = should_play_slot(datetime.now(), last_slot_hour)

        except Exception as loop_error:
            print(f"[CRITICAL] Scheduler loop exception: {loop_error}")
            time.sleep(10)


if __name__ == "__main__":
    print("Project Aries - Hourglass")
    print("Maintained by Physics Prop")
    try:
        scheduler()
    except KeyboardInterrupt:
        print("\\n[EXIT] Scheduler interrupted by user.")
    except Exception as fatal_error:
        print(f"[FATAL] Unhandled exception: {fatal_error}")
