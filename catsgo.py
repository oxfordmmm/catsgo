#! /usr/bin/env python3

import argh
import requests
import pickle
import json
import datetime
import time

from getpass import getpass

def load_config(config_file):
    with open(config_file) as f:
        cfg = json.loads(f.read())
    return cfg

config = load_config("config.json")
sp3_url = config["sp3_url"]
flow_name = config["clockwork_flow_name"]

session = requests.Session()

def save_cookies():
    with open('cookies', 'wb') as f:
        pickle.dump(session.cookies, f)

def load_cookies():
    with open('cookies', 'rb') as f:
        session.cookies.update(pickle.load(f))

def login():
    username = input(f"Username:")
    password = getpass()
    data = { 'username': username,
             'password': password }
    response =  session.post(sp3_url + '/login', data=data)
    save_cookies()

def logout():
    session = requests.Session()
    save_cookies()
    print("You are logged out.")

def check_login():
    load_cookies()
    url = sp3_url + '/am_i_logged_in'
    response = session.get(url)
    if 'html' in response.text:
        print(f'You are not logged in. Please login.')
        login()
    else:
        print(f'{ response.text } is logged in.')

def fetch(fetch_name):
    check_login()
    url =  sp3_url + f'/fetch_new'
    fetch_kind = 'local1'
    fetch_method = 'copy'
    response =  session.post(url, data = { 'fetch_name': fetch_name,
                           'fetch_kind': fetch_kind,
                           'fetch_method': fetch_method,
                           'is_api': True })
    return json.loads(response.text)

def check_fetch(fetch_uuid):
    check_login()
    url =  sp3_url + f'/fetch_details/{fetch_uuid}?api=v1'
    response =  session.get(url)
    return json.loads(response.text)

def run_clockwork(flow_name, fetch_uuid):
    check_login()
    url =  sp3_url + f'/flow/{ flow_name }/new'
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_name = f'sp3c-{ flow_name }-{ timestamp }'
    input_file_dir = f'/data/inputs/local/{ fetch_uuid }'

    data = { 'fetch_uuid':  fetch_uuid,
             'ref_uuid': "",
             'run_name':  run_name,
             'context': 'local',
             'kraken2_db-and---kraken2_db':  config["kraken2_db"],
             'indir-and---indir':  input_file_dir,
             'input_filetype-and---input_filetype':  config["input_file_type"],
             'readpat-and---readpat':  config["read_pattern"],
             'ref-and---ref': config["reference"],
             'save_rmdup_bam-and---save_rmdup_bam': config["save_rmdup_bam"],
             'save_samtools_gvcf-and---save_samtools_gvcf': config["save_samtools_gvcf"],
             'api': 'v1'
            }

    response =  session.post(url, data=data)

    return json.loads(response.text)

def check_run(flow_name, run_uuid):
    check_login()
    url =  sp3_url + f'/flow/{ flow_name }/details/{ run_uuid }?api=v1'
    response =  session.get(url)

    data = json.loads(response.text)
    if not data['data']:
        return "Error"
        return
    status = data['data'][0][3]
    if status == '-':
        return "Running"
    else:
        return status

def download_url(run_uuid):
    check_login()
    url =  f"{ sp3_url }/files/{ run_uuid }/"
    print(url)

def download_cmd(run_uuid):
    check_login()
    url =  f"wget -m -nH --cut-dirs=1 -np -R 'index.*' { sp3_url }/files/{ run_uuid }/"
    return(url)

def go(fetch_name):
    check_login()
    print(f'Fetching { fetch_name }')
    response =  fetch(fetch_name)
    fetch_uuid = response['guid']

    while True:
        check_login()
        response =  check_fetch(fetch_uuid)
        if response['status'] == 'failed':
            print('fetch failed: check web site for log')
            return
        if response['status'] == 'success':
            break
        time.sleep(1)

    if response['total'] == 0:
        print(f'Error: empty dataset. Fetch ID: {fetch_uuid}')
        return

    print(f'Dataset fetched successfully. Fetch ID: {fetch_uuid}')

    response =  run_clockwork(flow_name, fetch_uuid)
    run_uuid = response['run_uuid']

    print(f'Running clockwork. Run ID: {run_uuid}')

    while True:
        response =  check_run(flow_name, run_uuid)
        if response == 'ERR' or response == "Error":
            print('run failed: check web site for log')
            return
        if response == 'OK':
            break
        time.sleep(5)

    print(f'Run completed successfully.')
    print(f'Downloading run output.')

    cmd = download_cmd(run_uuid)
    print(f"{cmd} | bash")
    
if __name__ == "__main__":
    parser = argh.ArghParser()
    parser.add_commands([login, logout, fetch,
                         check_login, check_fetch, check_run,
                         download_cmd, download_url,
                         run_clockwork, go])
    parser.dispatch()
