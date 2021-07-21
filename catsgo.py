#! /usr/bin/env python3

import pickle
import json
import datetime
import time
import sys
import logging

import argh
import requests

def load_config(config_file):
    with open(config_file) as f:
        cfg = json.loads(f.read())
    return cfg

config = load_config("config.json")
sp3_url = config["sp3_url"]

session = requests.Session()

def save_cookies():
    with open('cookies', 'wb') as f:
        pickle.dump(session.cookies, f)

def load_cookies():
    with open('cookies', 'rb') as f:
        session.cookies.update(pickle.load(f))

def login():
    try:
        load_cookies()
    except:
        pass
    response = session.get(sp3_url + '/am_i_logged_in')
    if response.text == 'yes':
        return
    else:
        data = { 'username': config['username'],
                 'password': config['password'] }
        response =  session.post(sp3_url + '/login?api=v1', data=data)
        response = session.get(sp3_url + '/am_i_logged_in')
        if response.text != 'yes':
            sys.stderr.write("Error: Couldn't log in\n")
            sys.exit(1)
        save_cookies()

def fetch(fetch_name):
    login()
    url =  sp3_url + f'/fetch_new'
    fetch_kind = 'local1'
    fetch_method = config['fetch_method']
    response =  session.post(url, data = { 'fetch_name': fetch_name,
                           'fetch_kind': fetch_kind,
                           'fetch_method': fetch_method,
                           'is_api': True })
    return json.loads(response.text)

def try_n_times(proc, n, sleep_dur):
    attempt = 0
    while True:
        attempt = attempt + 1
        if attempt > 10:
            logging.error(f"Error: failed { proc.__name__ } after { n } tries.")
            sys.exit(1)
        try:
            return proc()
        except Exception as e:
            logging.warning(f"Warning: failed { proc.__name__ } attempt { attempt }. Exception: { str(e) }. Retrying after { sleep_dur } seconds.")
            time.sleep(sleep_dur)

def check_fetch(fetch_uuid):
    def check_fetch_inner():
        login()
        url =  sp3_url + f'/fetch_details/{fetch_uuid}?api=v1'
        response =  session.get(url)
        return json.loads(response.text)
    return try_n_times(check_fetch_inner, 10, 60)

def run_clockwork(flow_name, fetch_uuid):
    login()
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

def run_covid_illumina(flow_name, input_dir):
    url =  sp3_url + f'/flow/{ flow_name }/new'
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_name = f'sp3c-{ flow_name }-{ timestamp }'
    input_file_dir = f'{ input_dir }'

    data = { 'fetch_uuid': '',
             'run_name':  run_name,
             'context': 'local',
             'indir-and---directory':  input_file_dir,
             'readpat-and---pattern':  config["pattern"],
             'api': 'v1'
            }

    login()
    response =  session.post(url, data=data)
    return json.loads(response.text)

def run_covid_illumina_catsup(flow_name, indir, bucket_name, catsup_uuid):
    url =  sp3_url + f'/flow/{ flow_name }/new'
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_name = f'catsup_{catsup_uuid}'

    data = { 'fetch_uuid': '',
             'run_name':  run_name,
             'context': 'local',
             'objstore-and---objstore': 'false',
             'catsup-and---catsup': indir,
             'bucket-name-and---bucket': bucket_name,
             'varcaller-and---varCaller': 'viridian',
             'api': 'v1',
            }

    login()
    response =  session.post(url, data=data)
    return json.loads(response.text)


def get_run_uuids(flow_name):
    url = sp3_url + f"/flow/{ flow_name }?api=v2"
    login()
    response = session.get(url)
    return response.json()


def check_run(flow_name, run_uuid):
    def check_run_inner():
        login()
        url =  sp3_url + f'/flow/{ flow_name }/details/{ run_uuid }?api=v1'
        response =  session.get(url)

        data = json.loads(response.text)['data']
        if not data['data']:
            return "Error"
        status = data['data'][0][3]
        if status == '-':
            return "Running"
        else:
            return status
    return try_n_times(check_run_inner, 10, 60)

def check_run_resume(run_uuid):
    while True:
        response =  check_run(config['clockwork_flow_name'], run_uuid)
        print(f'Run {run_uuid} is {response}')
        if response == 'ERR' or response == "Error":
            print('run failed: check web site for log')
            return
        if response == 'OK':
            break
        time.sleep(5 * 60)

def download_url(run_uuid):
    login()
    url =  f"{ sp3_url }/files/{ run_uuid }/"
    print(url)

def download_cmd(run_uuid):
    login()
    url =  f"wget -m -nH --cut-dirs=1 -np -R 'index.*' { sp3_url }/files/{ run_uuid }/"
    return(url)

def download_report(run_uuid, dataset_id, do_print=True):
    def download_report_inner():
        login()
        url = f"{ sp3_url }/flow/{ run_uuid }/{ dataset_id }/report?api=v1"
        response = session.get(url)
        if do_print:
            return json.dumps(response.json(), indent=4)
        else:
            return response.json()
    return try_n_times(download_report_inner, 10, 60)

def run_info(flow_name, run_uuid, do_print=True):
    def run_info_inner():
        login()
        url = f"{ sp3_url }/flow/{ flow_name }/details/{ run_uuid }?api=v1"
        response = session.get(url)
        if do_print:
            return json.dumps(response.json(), indent=4)
        else:
            return response.json()
    return try_n_times(run_info_inner, 10, 60)

def download_reports(flow_name, run_uuid):
    login()
    info = run_info(flow_name, run_uuid, do_print=False)
    sample_names = list(info["trace_nice"].keys())
    sample_names.remove('unknown')
    sys.stderr.write(f"downloading { len(sample_names) } reports\n")
    out = dict()
    for i, sample_name in enumerate(sample_names):
        sys.stderr.write(f"Downloading report {i}. { run_uuid }/{ sample_name }\n")
        out[sample_name] = download_report(run_uuid, sample_name, do_print=False)
        sys.stderr.write(f"Downloaded report {i}. { run_uuid }/{ sample_name } len: { len(out[sample_name]) }\n")
    print(json.dumps(out, indent=4))

def download_nextflow_task_data(flow_name, run_uuid, do_print=True):
    login()
    url = f"{ sp3_url }/flow/{ flow_name }/report/{ run_uuid }?api=v1"
    response = session.get(url)
    if do_print:
        return response.text
    else:
        return response.json()

def download_nextflow_task_data_csv(flow_name, run_uuid, do_print=True):
    login()
    url = f"{ sp3_url }/flow/{ flow_name }/report/{ run_uuid }?api=v1"
    response = session.get(url)
    import pandas, io
    trace = json.dumps(json.loads(response.text)['trace'])
    table = pandas.read_json(io.StringIO(trace))
    table = table.drop(["script", "env"], axis=1)
    return table.sort_values("tag").to_csv(index=False)

def go(fetch_name):
    login()
    print(f'Fetching { fetch_name }')
    response =  fetch(fetch_name)
    fetch_uuid = response['guid']

    while True:
        response =  check_fetch(fetch_uuid)
        if response['status'] == 'failed':
            print('fetch failed: check web site for log')
            return
        if response['status'] == 'success':
            break
        time.sleep(5 * 60)

    if response['total'] == 0:
        print(f'Error: empty dataset. Fetch ID: {fetch_uuid}')
        return

    print(f'Dataset fetched successfully. Fetch ID: {fetch_uuid}')

    response =  run_clockwork(config['clockwork_flow_name'], fetch_uuid)
    run_uuid = response['run_uuid']

    print(f'Running clockwork. Run ID: {run_uuid}')

    while True:
        response =  check_run(config['clockwork_flow_name'], run_uuid)
        if response == 'ERR' or response == "Error":
            print('run failed: check web site for log')
            return
        if response == 'OK':
            break
        time.sleep(5 * 60)

    print(f'Run completed successfully.')
    print(f'Downloading run output.')

    cmd = download_cmd(run_uuid)
    print(f"{cmd} | bash")

if __name__ == "__main__":
    parser = argh.ArghParser()
    parser.add_commands([login, fetch, check_run_resume,
                         check_fetch, check_run, download_reports,
                         download_cmd, download_url, run_info,
                         run_clockwork, go,
                         download_report, download_nextflow_task_data, download_nextflow_task_data_csv, run_covid_illumina, run_covid_illumina_catsup, get_run_uuids])
    parser.dispatch()
