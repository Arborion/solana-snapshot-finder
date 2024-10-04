import os
import glob
import requests
import time
import math
import json
import sys
import argparse
import logging
import subprocess
from pathlib import Path
from requests import ReadTimeout, ConnectTimeout, HTTPError, Timeout, ConnectionError
from tqdm import tqdm
from multiprocessing.dummy import Pool as ThreadPool
import statistics

parser = argparse.ArgumentParser(description='Solana snapshot finder')
parser.add_argument('-t', '--threads-count', default=1000, type=int,
    help='The number of concurrently running threads that check snapshots for RPC nodes')
parser.add_argument('-r', '--rpc_address',
    default='https://api.mainnet-beta.solana.com', type=str,
    help='RPC address of the node from which the current slot number will be taken')
parser.add_argument('--max_snapshot_age', default=1300, type=int,
    help='How many slots ago the snapshot was created (in slots)')
parser.add_argument('--min_download_speed', default=60, type=int,
    help='Minimum average snapshot download speed in megabytes')
parser.add_argument('--max_download_speed', type=int,
    help='Maximum snapshot download speed in megabytes')
parser.add_argument('--max_latency', default=40, type=int,
    help='The maximum value of latency (milliseconds). If latency > max_latency --> skip')
parser.add_argument('--with_private_rpc', action="store_true",
    help='Enable adding and checking RPCs with private addresses')
parser.add_argument('--measurement_time', default=7, type=int,
    help='Time in seconds during which the script will measure the download speed')
parser.add_argument('--snapshot_path', type=str, default=".",
    help='The location where the snapshot will be downloaded (absolute path)')
parser.add_argument('--num_of_retries', default=5, type=int,
    help='The number of retries if a suitable server for downloading the snapshot was not found')
parser.add_argument('--sleep', default=30, type=int,
    help='Sleep before next retry (seconds)')
parser.add_argument('--sort_order', default='slots_diff', type=str,
    help='Priority way to sort the found servers: latency or slots_diff')
parser.add_argument('-b', '--blacklist', default='', type=str,
    help='Exclude specific snapshots by slot number or hash. Example: -b 135501350,135501360')
parser.add_argument('-v', '--verbose', help="Increase output verbosity to DEBUG", action="store_true")
parser.add_argument('--max_version', default='1.99.99', type=str,
    help='Maximum acceptable Solana version')

args = parser.parse_args()

# Global constants and variables
DEFAULT_HEADERS = {"Content-Type": "application/json"}
RPC = args.rpc_address
WITH_PRIVATE_RPC = args.with_private_rpc
MAX_SNAPSHOT_AGE_IN_SLOTS = args.max_snapshot_age
THREADS_COUNT = args.threads_count
MIN_DOWNLOAD_SPEED_MB = args.min_download_speed
MAX_DOWNLOAD_SPEED_MB = args.max_download_speed
SPEED_MEASURE_TIME_SEC = args.measurement_time
MAX_LATENCY = args.max_latency
SNAPSHOT_PATH = args.snapshot_path.rstrip('/')
NUM_OF_MAX_ATTEMPTS = args.num_of_retries
SLEEP_BEFORE_RETRY = args.sleep
NUM_OF_ATTEMPTS = 1
SORT_ORDER = args.sort_order
BLACKLIST = str(args.blacklist).split(",")
AVERAGE_SNAPSHOT_FILE_SIZE_MB = 2500.0
AVERAGE_INCREMENT_FILE_SIZE_MB = 200.0
AVERAGE_CATCHUP_SPEED = 2.0
FULL_LOCAL_SNAP_SLOT = 0
MAX_VERSION = args.max_version
DISCARDED_BY_VERSION = 0

current_slot = 0
DISCARDED_BY_ARCHIVE_TYPE = 0
DISCARDED_BY_LATENCY = 0
DISCARDED_BY_SLOT = 0
DISCARDED_BY_UNKNW_ERR = 0
DISCARDED_BY_TIMEOUT = 0
FULL_LOCAL_SNAPSHOTS = []
unsuitable_servers = set()
json_data = {
    "last_update_at": 0.0,
    "last_update_slot": 0,
    "total_rpc_nodes": 0,
    "rpc_nodes_with_actual_snapshot": 0,
    "rpc_nodes": []
}

# Configure Logging
logging.getLogger('urllib3').setLevel(logging.WARNING)
if args.verbose:
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(f'{SNAPSHOT_PATH}/snapshot-finder.log'),
            logging.StreamHandler(sys.stdout),
        ]
    )
else:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(f'{SNAPSHOT_PATH}/snapshot-finder.log'),
            logging.StreamHandler(sys.stdout),
        ]
    )
logger = logging.getLogger(__name__)

def convert_size(size_bytes):
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}"

def measure_speed(url: str, measure_time: int) -> float:
    logging.debug('measure_speed()')
    url = f'http://{url}/snapshot.tar.bz2'
    r = requests.get(url, stream=True, timeout=measure_time+2)
    r.raise_for_status()
    start_time = time.monotonic_ns()
    last_time = start_time
    loaded = 0
    speeds = []
    for chunk in r.iter_content(chunk_size=81920):
        curtime = time.monotonic_ns()
        worktime = (curtime - start_time) / 1_000_000_000
        if worktime >= measure_time:
            break
        delta = (curtime - last_time) / 1_000_000_000
        loaded += len(chunk)
        if delta > 1:
            estimated_bytes_per_second = loaded * (1 / delta)
            speeds.append(estimated_bytes_per_second)
            last_time = curtime
            loaded = 0
    return statistics.median(speeds)

def do_request(url_: str, method_: str = 'GET', data_: str = '', timeout_: int = 3,
               headers_: dict = None):
    global DISCARDED_BY_UNKNW_ERR
    global DISCARDED_BY_TIMEOUT
    r = ''
    if headers_ is None:
        headers_ = DEFAULT_HEADERS
    try:
        if method_.lower() == 'get':
            r = requests.get(url_, headers=headers_, timeout=(timeout_, timeout_))
        elif method_.lower() == 'post':
            r = requests.post(url_, headers=headers_, data=data_, timeout=(timeout_, timeout_))
        elif method_.lower() == 'head':
            r = requests.head(url_, headers=headers_, timeout=(timeout_, timeout_))
        return r
    except (ReadTimeout, ConnectTimeout, HTTPError, Timeout, ConnectionError) as reqErr:
        DISCARDED_BY_TIMEOUT += 1
        return f'error in do_request(): {reqErr}'
    except Exception as unknwErr:
        DISCARDED_BY_UNKNW_ERR += 1
        return f'error in do_request(): {unknwErr}'

def get_current_slot():
    logger.debug("get_current_slot()")
    d = '{"jsonrpc":"2.0","id":1, "method":"getSlot"}'
    try:
        r = do_request(url_=RPC, method_='post', data_=d, timeout_=25)
        if 'result' in str(r.text):
            return r.json()["result"]
        else:
            logger.error("Can't get current slot")
            logger.debug(r.text)
            return None
    except Exception as unknwErr:
        logger.error("Can't get current slot")
        logger.debug(unknwErr)
        return None

def get_all_rpc_ips():
    logger.debug("get_all_rpc_ips()")
    d = '{"jsonrpc":"2.0", "id":1, "method":"getClusterNodes"}'
    r = do_request(url_=RPC, method_='post', data_=d, timeout_=25)
    if 'result' in str(r.text):
        rpc_nodes = []
        for node in r.json()["result"]:
            rpc_address = None
            if WITH_PRIVATE_RPC:
                if node["rpc"] is not None:
                    rpc_address = node["rpc"]
                else:
                    gossip_ip = node["gossip"].split(":")[0]
                    rpc_address = f'{gossip_ip}:8899'
            else:
                if node["rpc"] is not None:
                    rpc_address = node["rpc"]
            if rpc_address:
                rpc_nodes.append({
                    'rpc': rpc_address,
                    'version': node.get('version', None)
                })
        return rpc_nodes
    else:
        logger.error(f"Can't get RPC IP addresses {r.text}")
        sys.exit()

def is_version_suitable(version: str):
    max_version = MAX_VERSION
    try:
        version_str = version.split()[0]  # Remove any suffixes after space
        version_parts = version_str.split('.')
        max_version_parts = max_version.split('.')
        # Pad with zeros to make lengths equal
        max_length = max(len(version_parts), len(max_version_parts))
        version_parts.extend(['0'] * (max_length - len(version_parts)))
        max_version_parts.extend(['0'] * (max_length - len(max_version_parts)))
        version_numbers = [int(part) for part in version_parts]
        max_version_numbers = [int(part) for part in max_version_parts]
        return version_numbers <= max_version_numbers
    except Exception as e:
        logger.debug(f'Error comparing versions: {e}')
        return False

def get_snapshot_slot(rpc_node: dict):
    global FULL_LOCAL_SNAP_SLOT
    global DISCARDED_BY_ARCHIVE_TYPE
    global DISCARDED_BY_LATENCY
    global DISCARDED_BY_SLOT
    global DISCARDED_BY_VERSION

    rpc_address = rpc_node['rpc']
    version = rpc_node.get('version', None)

    pbar.update(1)
    url = f'http://{rpc_address}/snapshot.tar.bz2'
    inc_url = f'http://{rpc_address}/incremental-snapshot.tar.bz2'

    # Check the version before proceeding
    if version is None:
        DISCARDED_BY_VERSION += 1
        logger.debug(f'Node {rpc_address} version is None, discarding')
        return
    elif not is_version_suitable(version):
        DISCARDED_BY_VERSION += 1
        logger.debug(f'Node {rpc_address} version {version} exceeds maximum allowed version {MAX_VERSION}, discarding')
        return

    try:
        # Check incremental snapshot
        r = do_request(url_=inc_url, method_='head', timeout_=1)
        if isinstance(r, str):
            return
        if 'location' in r.headers and 'error' not in str(r.text):
            if r.elapsed.total_seconds() * 1000 > MAX_LATENCY:
                DISCARDED_BY_LATENCY += 1
                return
            snap_location_ = r.headers["location"]
            if snap_location_.endswith('tar'):
                DISCARDED_BY_ARCHIVE_TYPE += 1
                return
            incremental_snap_slot = int(snap_location_.split("-")[2])
            snap_slot_ = int(snap_location_.split("-")[3])
            slots_diff = current_slot - snap_slot_
            if slots_diff < -100 or slots_diff > MAX_SNAPSHOT_AGE_IN_SLOTS:
                DISCARDED_BY_SLOT += 1
                return
            if FULL_LOCAL_SNAP_SLOT == incremental_snap_slot:
                json_data["rpc_nodes"].append({
                    "snapshot_address": rpc_address,
                    "slots_diff": slots_diff,
                    "latency": r.elapsed.total_seconds() * 1000,
                    "files_to_download": [snap_location_],
                    "cost": AVERAGE_INCREMENT_FILE_SIZE_MB / MIN_DOWNLOAD_SPEED_MB + slots_diff / AVERAGE_CATCHUP_SPEED
                })
                return
            r2 = do_request(url_=url, method_='head', timeout_=1)
            if 'location' in r2.headers and 'error' not in str(r2.text):
                json_data["rpc_nodes"].append({
                    "snapshot_address": rpc_address,
                    "slots_diff": slots_diff,
                    "latency": r.elapsed.total_seconds() * 1000,
                    "files_to_download": [r2.headers["location"], r.headers["location"]],
                    "cost": (AVERAGE_SNAPSHOT_FILE_SIZE_MB + AVERAGE_INCREMENT_FILE_SIZE_MB) / MIN_DOWNLOAD_SPEED_MB + slots_diff / AVERAGE_CATCHUP_SPEED
                })
                return
        # Check full snapshot
        r = do_request(url_=url, method_='head', timeout_=1)
        if isinstance(r, str):
            return
        if 'location' in r.headers and 'error' not in str(r.text):
            if r.elapsed.total_seconds() * 1000 > MAX_LATENCY:
                DISCARDED_BY_LATENCY += 1
                return
            snap_location_ = r.headers["location"]
            if snap_location_.endswith('tar'):
                DISCARDED_BY_ARCHIVE_TYPE += 1
                return
            full_snap_slot_ = int(snap_location_.split("-")[1])
            slots_diff_full = current_slot - full_snap_slot_
            if slots_diff_full > MAX_SNAPSHOT_AGE_IN_SLOTS:
                DISCARDED_BY_SLOT += 1
                return
            json_data["rpc_nodes"].append({
                "snapshot_address": rpc_address,
                "slots_diff": slots_diff_full,
                "latency": r.elapsed.total_seconds() * 1000,
                "files_to_download": [snap_location_],
                "cost": AVERAGE_SNAPSHOT_FILE_SIZE_MB / MIN_DOWNLOAD_SPEED_MB + slots_diff_full / AVERAGE_CATCHUP_SPEED
            })
            return
    except Exception as getSnapErr_:
        return

def download(url: str):
    fname = url[url.rfind('/'):].replace("/", "")
    temp_fname = f'{SNAPSHOT_PATH}/tmp-{fname}'
    try:
        if MAX_DOWNLOAD_SPEED_MB is not None:
            process = subprocess.run(
                ['/usr/bin/wget', '--progress=dot:giga', f'--limit-rate={MAX_DOWNLOAD_SPEED_MB}M', '--trust-server-names', url, f'-O{temp_fname}'],
                stdout=subprocess.PIPE,
                universal_newlines=True
            )
        else:
            process = subprocess.run(
                ['/usr/bin/wget', '--progress=dot:giga', '--trust-server-names', url, f'-O{temp_fname}'],
                stdout=subprocess.PIPE,
                universal_newlines=True
            )
        logger.info(f'Rename the downloaded file {temp_fname} --> {fname}')
        os.rename(temp_fname, f'{SNAPSHOT_PATH}/{fname}')
    except Exception as unknwErr:
        logger.error(f'Exception in download() function. Make sure wget is installed\n{unknwErr}')

def main_worker():
    try:
        global FULL_LOCAL_SNAP_SLOT
        rpc_nodes = get_all_rpc_ips()
        global pbar
        pbar = tqdm(total=len(rpc_nodes))
        logger.info(f'RPC servers in total: {len(rpc_nodes)} | Current slot number: {current_slot}\n')

        # Search for full local snapshots.
        FULL_LOCAL_SNAPSHOTS = glob.glob(f'{SNAPSHOT_PATH}/snapshot-*tar*')
        if len(FULL_LOCAL_SNAPSHOTS) > 0:
            FULL_LOCAL_SNAPSHOTS.sort(reverse=True)
            FULL_LOCAL_SNAP_SLOT = FULL_LOCAL_SNAPSHOTS[0].replace(SNAPSHOT_PATH, "").split("-")[1]
            logger.info(f'Found full local snapshot {FULL_LOCAL_SNAPSHOTS[0]} | {FULL_LOCAL_SNAP_SLOT=}')
        else:
            logger.info(f"Can't find any full local snapshots in this path {SNAPSHOT_PATH} --> the search will be carried out on full snapshots")

        print('Searching information about snapshots on all found RPCs')
        pool = ThreadPool()
        pool.map(get_snapshot_slot, rpc_nodes)
        logger.info(f'Found suitable RPCs: {len(json_data["rpc_nodes"])}')
        logger.info('The following information shows for what reason and how many RPCs were skipped.'
                    ' Timeout most probably means that node RPC port does not respond (port is closed)\n'
                    f'{DISCARDED_BY_ARCHIVE_TYPE=} | {DISCARDED_BY_LATENCY=} |'
                    f' {DISCARDED_BY_SLOT=} | {DISCARDED_BY_TIMEOUT=} | {DISCARDED_BY_UNKNW_ERR=} | {DISCARDED_BY_VERSION=}') 

        if len(json_data["rpc_nodes"]) == 0:
            logger.info(f'No snapshot nodes were found matching the given parameters: {args.max_snapshot_age=}')
            sys.exit()

        # Sort list of RPC nodes by SORT_ORDER
        rpc_nodes_sorted = sorted(json_data["rpc_nodes"], key=lambda k: k[SORT_ORDER])

        json_data.update({
            "last_update_at": time.time(),
            "last_update_slot": current_slot,
            "total_rpc_nodes": len(rpc_nodes),
            "rpc_nodes_with_actual_snapshot": len(json_data["rpc_nodes"]),
            "rpc_nodes": rpc_nodes_sorted
        })

        with open(f'{SNAPSHOT_PATH}/snapshot.json', "w") as result_f:
            json.dump(json_data, result_f, indent=2)
        logger.info(f'All data is saved to JSON file - {SNAPSHOT_PATH}/snapshot.json')

        best_snapshot_node = {}
        num_of_rpc_to_check = 15

        logger.info("TRYING TO DOWNLOAD FILES")
        for i, rpc_node in enumerate(json_data["rpc_nodes"], start=1):
            # Filter blacklisted snapshots
            if BLACKLIST != ['']:
                if any(b in str(rpc_node["files_to_download"]) for b in BLACKLIST):
                    logger.info(f'{i}\\{len(json_data["rpc_nodes"])} BLACKLISTED --> {rpc_node}')
                    continue

            logger.info(f'{i}\\{len(json_data["rpc_nodes"])} checking the speed {rpc_node}')
            if rpc_node["snapshot_address"] in unsuitable_servers:
                logger.info(f'RPC node already in unsuitable list --> skip {rpc_node["snapshot_address"]}')
                continue

            down_speed_bytes = measure_speed(url=rpc_node["snapshot_address"], measure_time=SPEED_MEASURE_TIME_SEC)
            down_speed_mb = convert_size(down_speed_bytes)
            if down_speed_bytes < MIN_DOWNLOAD_SPEED_MB * 1e6:
                logger.info(f'Too slow: {rpc_node=} {down_speed_mb=}')
                unsuitable_servers.add(rpc_node["snapshot_address"])
                continue
            elif down_speed_bytes >= MIN_DOWNLOAD_SPEED_MB * 1e6:
                logger.info(f'Suitable snapshot server found: {rpc_node=} {down_speed_mb=}')
                for path in reversed(rpc_node["files_to_download"]):
                    # Do not download full snapshot if it already exists locally
                    if str(path).startswith("/snapshot-"):
                        full_snap_slot__ = path.split("-")[1]
                        if full_snap_slot__ == FULL_LOCAL_SNAP_SLOT:
                            continue
                    if 'incremental' in path:
                        r = do_request(f'http://{rpc_node["snapshot_address"]}/incremental-snapshot.tar.bz2', method_='head', timeout_=2)
                        if 'location' in str(r.headers) and 'error' not in str(r.text):
                            best_snapshot_node = f'http://{rpc_node["snapshot_address"]}{r.headers["location"]}'
                        else:
                            best_snapshot_node = f'http://{rpc_node["snapshot_address"]}{path}'
                    else:
                        best_snapshot_node = f'http://{rpc_node["snapshot_address"]}{path}'
                    logger.info(f'Downloading {best_snapshot_node} snapshot to {SNAPSHOT_PATH}')
                    download(url=best_snapshot_node)
                return 0
            elif i > num_of_rpc_to_check:
                logger.info(f'The limit on the number of RPC nodes from which we measure the speed has been reached {num_of_rpc_to_check=}\n')
                break
            else:
                logger.info(f'{down_speed_mb=} < {MIN_DOWNLOAD_SPEED_MB=}')

        if best_snapshot_node == {}:
            logger.error('No snapshot nodes were found matching the given parameters.'
                         f'\nTry restarting the script with --with_private_rpc'
                         f'RETRY #{NUM_OF_ATTEMPTS}\\{NUM_OF_MAX_ATTEMPTS}')
            return 1
    except KeyboardInterrupt:
        sys.exit('\nKeyboardInterrupt - ctrl + c')
    except Exception as e:
        logger.error(f'Exception in main_worker(): {e}')
        return 1

# Initial Logging
logger.info("Version: 0.3.4")
logger.info("https://github.com/c29r3/solana-snapshot-finder\n\n")
logger.info(f'{RPC=}\n'
      f'{MAX_SNAPSHOT_AGE_IN_SLOTS=}\n'
      f'{MIN_DOWNLOAD_SPEED_MB=}\n'
      f'{MAX_DOWNLOAD_SPEED_MB=}\n'
      f'{SNAPSHOT_PATH=}\n'
      f'{THREADS_COUNT=}\n'
      f'{NUM_OF_MAX_ATTEMPTS=}\n'
      f'{WITH_PRIVATE_RPC=}\n'
      f'{SORT_ORDER=}\n'
      f'{MAX_VERSION=}')

# Check write permissions
try:
    f_ = open(f'{SNAPSHOT_PATH}/write_perm_test', 'w')
    f_.close()
    os.remove(f'{SNAPSHOT_PATH}/write_perm_test')
except IOError:
    logger.error(f'\nCheck {SNAPSHOT_PATH=} and permissions')
    Path(SNAPSHOT_PATH).mkdir(parents=True, exist_ok=True)

while NUM_OF_ATTEMPTS <= NUM_OF_MAX_ATTEMPTS:
    current_slot = get_current_slot()
    logger.info(f'Attempt number: {NUM_OF_ATTEMPTS}. Total attempts: {NUM_OF_MAX_ATTEMPTS}')
    NUM_OF_ATTEMPTS += 1

    if current_slot is None:
        continue

    worker_result = main_worker()

    if worker_result == 0:
        logger.info("Done")
        exit(0)

    if worker_result != 0:
        logger.info("Now trying with flag --with_private_rpc")
        WITH_PRIVATE_RPC = True

    if NUM_OF_ATTEMPTS >= NUM_OF_MAX_ATTEMPTS:
        logger.error('Could not find a suitable snapshot --> exit')
        sys.exit()
    
    logger.info(f"Sleeping {SLEEP_BEFORE_RETRY} seconds before next try")
    time.sleep(SLEEP_BEFORE_RETRY)
