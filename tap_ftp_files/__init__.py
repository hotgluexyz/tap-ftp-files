#!/usr/bin/env python3
import logging
import os
import json
import argparse
from tap_ftp_files.client import connection
import pytz
from datetime import datetime
logger = logging.getLogger("tap-ftp-files")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def load_json(path):
    if not os.path.exists(path):
        return dict()

    with open(path) as f:
        return json.load(f)

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config',
        help='Config file',
        required=True)

    parser.add_argument(
        '-s', '--state',
        help='State file',
        required=False)

    args = parser.parse_args()
    if args.config:
        setattr(args, 'config_path', args.config)
        args.config = load_json(args.config)

    if args.state:
        setattr(args, 'state_path', args.state)
        args.state = load_json(args.state)

    return args

def download(args):
    logger.debug(f"Downloading data...")
    config = args.config
    state = args.state or dict()
    target_dir = config.get('target_dir')
    file_groups = config.get('file_groups')
    incremental_mode = config.get('incremental_mode') == True

    conn = connection(config)
    if incremental_mode:
        if state.get('start_date'):
            start_date = state.get('start_date')
        else:
            start_date = config.get('start_date')
    else:
        start_date = None

    if start_date:
        if start_date.endswith('Z'):
            start_date = start_date[:-1]
        start_date = datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%S.%f')

    
    for file_group in file_groups:
        remote_path = file_group.get('remote_path')
        search_pattern = file_group.get('search_pattern', "") # Regex


        logger.info(f"Downloading: data from {remote_path} -> {target_dir}")
            
        files = conn.get_files(remote_path, search_pattern, start_date)
        for file in files:
            logger.info(f"Downloading: {file['filepath']}")
            with conn.get_file_handle(file) as file_handle:
                local_file_path = os.path.join(target_dir, os.path.basename(file['filepath']))
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                with open(local_file_path, 'wb') as local_file:
                    local_file.write(file_handle.read())
            
    logger.info(f"Data downloaded.")

    # Write start_date to state file
    files_with_last_modified = [f for f in files if f['last_modified'] is not None]
    if files_with_last_modified:
    
        last_modified = max([f['last_modified'] for f in files_with_last_modified])

        state['start_date'] = last_modified.strftime('%Y-%m-%dT%H:%M:%S.%f')
        with open(args.state_path, 'w') as f:
            json.dump(state, f)

        logger.info(f"State file updated with start_date: {last_modified}")

def main():
    args = parse_args()
    download(args)

if __name__ == "__main__":
    main()
