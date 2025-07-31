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
import logging, coloredlogs


EXEC_DIR = Path(__file__).resolve().parent
log = logging.getLogger("hourglass")
log.setLevel(logging.DEBUG)
coloredlogs.install(level="DEBUG", logger=log)

PLAYER_SLEEP = 1
VIDEO_EXTENSIONS = {".mp4", ".mkv", ".mov", ".ts", ".avi"}
QUEUE_MAX_SIZE = 5

recent_fillers = deque(maxlen=5)
play_queue = []

current_show_index = 0
episodes_played_from_show = 0


def normalize_path(path):
    try:
        if isinstance(path, str):
            path = Path(path)
        return str(path.resolve()).replace("\\", "/")
    except Exception as e:
        log.error(f"Failed to normalize path: {e}")
        return str(path)


try:
    with open(f"{EXEC_DIR}\config.json", "r") as conf_file:
        conf = json.load(conf_file)
    SLOT_MINUTE = conf["SLOT_MINUTE"]
    SLOT_DURATION = conf["SLOT_DURATION"]
    COMMERCIAL_PADDING = conf["COMMERCIAL_PADDING"]
    EPISODES_FOLDER = Path(conf["EPISODES_FOLDER"])
    FILLER_FOLDER = Path(conf["FILLER_FOLDER"])
    SLOT_FOLDER = Path(conf["SLOT_FOLDER"])
    CASPAR_HOST = conf["CASPAR_HOST"]
    CASPAR_PORT = conf["CASPAR_PORT"]
    EPISODES_PER_SHOW = conf["EPISODES_PER_SHOW"]
except:
    log.error("Cant load config.")
    exit()


def get_show_folders():
    try:
        return [f for f in EPISODES_FOLDER.iterdir() if f.is_dir()]
    except Exception as e:
        log.error(f"Failed to list show folders: {e}")
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
            log.error(f"Failed to send command '{cmd}': {e}")
            return None

    def play_video(
        self, path, channel=1, layer=10, audio_channels=None, audio_map=None
    ):
        cmd = f'PLAY {channel}-{layer} "{normalize_path(path)}"'
        if audio_channels:
            cmd += f" --audioChannels {audio_channels}"
        if audio_map:
            cmd += f" --audioMap {audio_map}"
        return self.send_command(cmd)

    def overlay_caption(self, text, channel=1, layer=20, template="timestamp_template"):
        json_data = f'{{"text":"{text}"}}'
        return self.send_command(
            f'CG ADD {channel}-{layer} 0 "{template}" 1 "{json_data}"'
        )


def get_random_slot_ts():
    try:
        slot_files = sorted(SLOT_FOLDER.glob("*.ts"))
        if not slot_files:
            raise FileNotFoundError("No .ts files found in slot folder.")
        return random.choice(slot_files)
    except Exception as e:
        log.error(f"Slot TS selection failed: {e}")
        return get_random_filler()


def time_until_next_slot():
    try:
        now = datetime.now()
        next_slot = now.replace(minute=SLOT_MINUTE, second=0, microsecond=0)
        if now.minute > SLOT_MINUTE or (now.minute == SLOT_MINUTE and now.second > 0):
            next_slot += timedelta(hours=1)
        return max(0, (next_slot - now).total_seconds())
    except Exception as e:
        log.error(f"Failed to calculate time until slot: {e}")
        return SLOT_DURATION


duration_cache = {}


def get_video_duration(file_path):
    # print(f"[DEBUG] Probing duration for: {file_path}")
    file_path = str(file_path)
    if file_path in duration_cache:
        return duration_cache[file_path]

    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "json",
                file_path,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=5,
        )
        data = json.loads(result.stdout)
        duration = float(data["format"]["duration"])
        duration_cache[file_path] = duration
        return duration
    except Exception as e:
        log.error(f"Could not get duration for '{file_path}': {e}")
        duration_cache[file_path] = 0
        return 0


def play_video(video_path, duration, label):
    try:
        ts = int(time.time())
        # caspar.overlay_caption(f"{label} - Timestamp: {ts}")
        caspar.play_video(video_path)
        time.sleep(PLAYER_SLEEP)
    except Exception as e:
        log.error(f"Playback failed for '{video_path}': {e}")


def get_next_random_episode(count=5):
    show_folders = [f for f in EPISODES_FOLDER.iterdir() if f.is_dir()]
    random.shuffle(show_folders)

    selected = []
    for folder in show_folders:
        episode_candidates = [
            f
            for f in folder.iterdir()
            if f.is_file()
            and f.suffix.lower() in VIDEO_EXTENSIONS
            and f.name != SLOT_VIDEO.name
        ]
        if not episode_candidates:
            continue
        episode = random.choice(episode_candidates)
        log.info(f"Adding {episode} to the queue")
        selected.append(Path(episode))
        if len(selected) >= count:
            break

    log.info(f"Queue filled.")
    return selected


def refill_queue():
    while len(play_queue) < QUEUE_MAX_SIZE:
        next_clip_list = get_next_random_episode()
        for next_clip in next_clip_list:
            if not next_clip or not next_clip.exists():
                continue
            duration = get_video_duration(next_clip)
            if duration <= 1:
                continue
            play_queue.append(
                {
                    "path": normalize_path(next_clip),
                    "type": (
                        "EPISODE" if next_clip.parent == EPISODES_FOLDER else "FILLER"
                    ),
                    "label": next_clip.name,
                    "duration": duration,
                }
            )


def get_random_filler():
    fillers = [
        f
        for f in FILLER_FOLDER.iterdir()
        if f.is_file()
        and f.suffix.lower() in VIDEO_EXTENSIONS
        and f not in recent_fillers
    ]
    if not fillers:
        recent_fillers.clear()
        fillers = [
            f
            for f in FILLER_FOLDER.iterdir()
            if f.is_file() and f.suffix.lower() in VIDEO_EXTENSIONS
        ]
    return random.choice(fillers) if fillers else None


def play_filler_until_slot(seconds_remaining):
    try:
        while seconds_remaining > SLOT_DURATION:
            filler = get_random_filler()
            if not filler or not filler.exists():
                log.warn("No valid filler found.")
                break
            duration = get_video_duration(filler)
            if duration <= 1 or duration >= seconds_remaining - 2:
                continue
            log.info(f"Playing filler: {filler.name} ({duration:.1f}s)")
            play_video(filler, duration, "FILLER")
            recent_fillers.append(filler)
            time.sleep(duration)
            seconds_remaining -= duration
    except Exception as e:
        log.error(f"Filler playback failed: {e}")


def play_commercial_block(duration):
    log.info(f"Playing commercial padding block (~{duration} sec)")
    play_filler_until_slot(duration)


def get_fitting_episode(max_duration):
    show_dirs = [d for d in EPISODES_FOLDER.iterdir() if d.is_dir()]
    random.shuffle(show_dirs)

    best_single = None
    smallest_gap = max_duration

    # Try to find best single episode
    for show in show_dirs:
        episodes = [
            f
            for f in show.iterdir()
            if f.is_file()
            and f.suffix.lower() in VIDEO_EXTENSIONS
            and f.name != SLOT_VIDEO.name
        ]
        random.shuffle(episodes)
        for ep in episodes:
            duration = get_video_duration(ep)
            if 1 < duration <= max_duration:
                gap = max_duration - duration
                if gap < smallest_gap:
                    best_single = [
                        {
                            "path": normalize_path(ep),
                            "type": "EPISODE",
                            "label": f"{show.name} - {ep.name}",
                            "duration": duration,
                        }
                    ]
                    smallest_gap = gap

    # Try to find best pair of episodes
    all_eps = [
        f
        for d in show_dirs
        for f in d.iterdir()
        if f.is_file()
        and f.suffix.lower() in VIDEO_EXTENSIONS
        and f.name != SLOT_VIDEO.name
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
                            "duration": d1,
                        },
                        {
                            "path": normalize_path(all_eps[j]),
                            "type": "EPISODE",
                            "label": all_eps[j].name,
                            "duration": d2,
                        },
                    ]
                    smallest_pair_gap = gap

    # If no pair or single episode fits, return a filler block
    if not best_pair and not best_single:
        filler = get_random_filler()
        if filler and filler.exists():
            duration = get_video_duration(filler)
            return [
                {
                    "path": normalize_path(filler),
                    "type": "FILLER",
                    "label": f"FALLBACK - {filler.name}",
                    "duration": duration,
                }
            ]
        else:
            log.warn("No fallback filler available.")
            return None

    return best_pair if best_pair else best_single


def scheduler():
    def is_slot_time(now, last_hour):
        remaining = time_until_next_slot()
        return (now.hour != last_hour) and abs(remaining) <= 60

    def execute_slot(now):
        log.info("Launching slot content")
        slot_clip = get_random_slot_ts()
        play_video(slot_clip, SLOT_DURATION, "SLOT")
        time.sleep(SLOT_DURATION)
        return now.hour

    def play_fallback_stack(seconds_to_slot):
        stack = get_random_episodes(count=5)
        for item in stack:
            if not Path(item["path"]).exists():
                log.warn(f"Skipping missing fallback: {item['label']}")
                continue

            if item["duration"] < seconds_to_slot - SLOT_DURATION:
                log.info(f"Fallback play: {item['label']} ({item['duration']:.1f}s)")
                play_video(item["path"], item["duration"], item["type"])
                time.sleep(item["duration"])
                seconds_to_slot = time_until_next_slot()

                if seconds_to_slot > SLOT_DURATION + COMMERCIAL_PADDING:
                    log.info(f"Padding with commercials: {COMMERCIAL_PADDING}s")
                    play_commercial_block(COMMERCIAL_PADDING)
                    seconds_to_slot = time_until_next_slot()
            else:
                log.info("Slot too close — switching to filler")
                play_filler_until_slot(seconds_to_slot - SLOT_DURATION)
                break

    last_slot_hour = None
    log.info(f"Scheduler booting at {datetime.now()}")

    startup_time = time.time()
    refill_queue()
    if time.time() - startup_time > 10:
        log.error("refill_queue took too long — possible hang")

    while True:
        try:
            now = datetime.now()

            if not EPISODES_FOLDER.exists():
                log.warn("Episodes folder missing. Waiting...")
                time.sleep(30)
                continue

            seconds_to_slot = time_until_next_slot()
            max_episode_duration = seconds_to_slot - SLOT_DURATION - 2

            fitting_episodes = None
            seconds_to_slot = time_until_next_slot()

            if (
                seconds_to_slot <= 600
            ):  # Only try fitting episodes within 10 minutes of slot
                max_episode_duration = seconds_to_slot - SLOT_DURATION - 2
                fitting_episodes = get_fitting_episode(max_episode_duration)

            if fitting_episodes:
                for ep in fitting_episodes:
                    if not Path(ep["path"]).exists():
                        log.warn(f"Skipping missing file: {ep['label']}")
                        continue

                    log.info(f"Playing fitting: {ep['label']} ({ep['duration']:.1f}s)")
                    play_video(ep["path"], ep["duration"], ep["type"])
                    time.sleep(max(0, ep["duration"] - 2))

                if is_slot_time(datetime.now(), last_slot_hour):
                    last_slot_hour = execute_slot(datetime.now())
                refill_queue()
                continue

            if is_slot_time(now, last_slot_hour):
                last_slot_hour = execute_slot(now)
                refill_queue()
                continue

            if not play_queue:
                refill_queue()

            current_item = play_queue.pop(0) if play_queue else None
            if not current_item:
                log.warn("No item in queue. Waiting briefly...")
                time.sleep(5)
                continue

            if not Path(current_item["path"]).exists():
                log.warn("Skipping missing queued item")
                continue

            if current_item["duration"] >= seconds_to_slot - SLOT_DURATION:
                log.info("Item too close to slot. Playing filler.")
                play_filler_until_slot(seconds_to_slot - SLOT_DURATION)
                last_slot_hour = execute_slot(datetime.now())
                refill_queue()
                continue

            log.info(f"Playing queued item: {current_item['label']}")
            play_video(
                current_item["path"], current_item["duration"], current_item["type"]
            )
            time.sleep(max(0, current_item["duration"] - 2))

            seconds_to_slot = time_until_next_slot()
            if (
                play_queue
                and play_queue[0]["duration"] < seconds_to_slot - SLOT_DURATION
            ):
                next_item = play_queue.pop(0)
                log.info(f"Playing next in queue: {next_item['label']}")
                play_video(next_item["path"], next_item["duration"], next_item["type"])
                time.sleep(next_item["duration"])
                refill_queue()
            else:
                log.info("Slot too close. Holding next queued item.")

            seconds_to_slot = time_until_next_slot()
            if seconds_to_slot > SLOT_DURATION + COMMERCIAL_PADDING:
                play_commercial_block(COMMERCIAL_PADDING)
                seconds_to_slot = time_until_next_slot()

            if seconds_to_slot > SLOT_DURATION:
                log.info(f"Playing filler until slot ({seconds_to_slot:.1f}s left)")
                play_filler_until_slot(seconds_to_slot - SLOT_DURATION)
                last_slot_hour = execute_slot(datetime.now())

        except Exception as loop_error:
            log.error(f"Scheduler loop exception: {loop_error}")
            time.sleep(10)


if __name__ == "__main__":
    log.info("Project Aries - Hourglass")
    log.info("Maintained by Physics Prop")
    caspar = CasparCGClient()
    SLOT_VIDEO = get_random_slot_ts()
    try:
        scheduler()
    except KeyboardInterrupt:
        log.info("Scheduler interrupted by user.")
    except Exception as fatal_error:
        log.error(f"Unhandled exception: {fatal_error}")
