import argh
from io import StringIO
from tabulate import tabulate
import requests
import pickle
import pandas
import json

sp3_url = 'https://cats.oxfordfun.com'
s = requests.Session()

def save_cookies():
    with open('cookies', 'wb') as f:
        pickle.dump(s.cookies, f)

def load_cookies():
    with open('cookies', 'rb') as f:
        s.cookies.update(pickle.load(f))

def login(username, password):
    data = { 'username': username,
             'password': password }
    r = s.post(sp3_url + '/login', data=data)
    save_cookies()

def dashboard():
    load_cookies()
    r = s.get(sp3_url + '/?api=v1')
    t = list()
    table_names = ['Running', 'Recent OK', 'Recent failed']
    headers=['Run UUID', 'Pipeline name', 'Run name']
    for table_name, table in zip(table_names, json.loads(r.text)):
        t = [[x[14], x[9], x[19]] for x in table]
        print(f'\n\t{table_name}\n')
        print(tabulate(t, tablefmt="fancy_grid", headers=headers))

def pipelines():
    load_cookies()
    r = s.get(sp3_url + '/flows?api=v1')
    t = list()
    headers=['Pipeline name', 'git version']
    for pipeline in json.loads(r.text):
        t.append([pipeline['name'], pipeline['git_version']])
    print(tabulate(t, tablefmt="fancy_grid", headers=headers))

def runs(pipeline_name):
    load_cookies()
    u = sp3_url + f'/flow/{pipeline_name}?api=v1'
    print(u)
    r = s.get(u)
    t = list()
    headers = ['Run UUID', 'Run name']
    for run in json.loads(r.text):
        t.append([run[14], run[19]])
    print(tabulate(t, tablefmt="fancy_grid", headers=headers))

def samples(pipeline_name, run_uuid):
    load_cookies()
    u = sp3_url + f'/flow/{pipeline_name}/details/{run_uuid}?api=v1'
    print(u)
    r = s.get(u)
    headers=['Sample name', 'tags']
    t = list()
    data = json.loads(r.text)
    sample_names = list(data['trace_nice'].keys())
    sample_names.remove('unknown')
    for sample_name in sample_names:
        if sample_name in data['tags']:
            tags_str = ', '.join([': '.join(x) for x in data['tags'][sample_name]])
            t.append([sample_name, tags_str])
        else:
            t.append(list())
    print(tabulate(t, tablefmt="fancy_grid", headers=headers))

def fetch(fetch_name):
    load_cookies()
    u = sp3_url + f'/fetch_new'
    fetch_kind = 'local1'
    fetch_method = 'copy'
    r = s.post(u, data = { 'fetch_name': fetch_name,
                           'fetch_kind': fetch_kind,
                           'fetch_method': fetch_method,
                           'is_api': True })

    print(json.dumps(json.loads(r.text), indent=4))

def is_fetch_ready(fetch_uuid):
    load_cookies()
    u = sp3_url + f'/fetch_details/{fetch_uuid}?api=v1'
    r = s.get(u)
    print(json.dumps(json.loads(r.text), indent=4))

def is_run_ready(flow_name, run_uuid):
    load_cookies()
    u = sp3_url + f'/flow/{ flow_name }/details/{ run_uuid }?api=v1'
    r = s.get(u)
    data = json.loads(r.text)
    print(data['data'][0][3])

if __name__ == "__main__":
    parser = argh.ArghParser()
    parser.add_commands([login, dashboard, pipelines, runs, samples, fetch,
                         is_fetch_ready, is_run_ready])
    parser.dispatch()
