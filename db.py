import logging
import requests
import json
import sys
import configparser
from datetime import datetime
from collections import KeysView


class Config:
    host = None
    user = None
    idcs = None

    def __init__(self, fn):
        c = configparser.ConfigParser()
        c.read_file(open(fn))
        self.host = c["oracle_rest"]["host"]
        self.user = c["oracle_rest"]["user"]
        self.idcs = c["oracle_rest"]["idcs"]


config = Config("config.ini")

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logging.getLogger().setLevel(logging.INFO)

def get_apex_token():
    with open("secrets.json") as f:
        c = json.load(f)
        client_id = c.get("client_id")
        client_secret = c.get("client_secret")

    access_token_response = requests.post(
        config.idcs,
        data={
            "grant_type": "client_credentials",
            "scope": config.host,
        },
        allow_redirects=False,
        auth=(client_id, client_secret),
    )
    access_token = access_token_response.json().get("access_token")
    return access_token

def update_sample(sample_id, data, apex_token, config=config):
    url = f"{config.host}/samples/{sample_id}"
    headers = {
        "Authorization": f"Bearer {apex_token}",
        "Content-type": "application/json",
    }
    response = requests.put(url, headers=headers, data=data)
    return response


def get_batches(apex_token, config=config):
    url = f"{config.host}/batches"
    method = "GET"
    headers = {"Authorization": f"Bearer {apex_token}"}
    response = requests.get(url, headers=headers)
    try:
        return response.json()
    except:
        logging.error(f"unexpected response from GET {url}: {response.text}")
        return []

def get_batch_samples(batch_id, apex_token, config=config):
    url = f"{config.host}/batches/{batch_id}"
    method = "GET"
    headers = {"Authorization": f"Bearer {apex_token}"}
    response = requests.get(url, headers=headers)
    try:
        return response.json()
    except:
        logging.error(f"unexpected response from GET {url}: {response.text}")
        return []

def get_sample(sample_id, apex_token, config=config):
    url = f"{config.host}/samples/{sample_id}"
    method = "GET"
    headers = {"Authorization": f"Bearer {apex_token}"}
    response = requests.get(url, headers=headers)
    #   assert print(response) # is 200
    # print(response)
    # print(response.text)
    try:
        return response.json()
    except:
        #        print(f"empty response from host: {url}", file=sys.stderr)
        return {}


def filter_sample(sample_id, pipeline_version, config=config):
    sample = get_sample(sample_id, config)
    if "analysis" in sample:
        if sample["analysis"]["pipelineVersion"] == pipeline_version:
            return None
        else:
            return sample_id
    else:
        return sample_id


def run_samples(pipeline_version, query=None, config=config):
    for sample in all_samples(query=query, config=config):
        print(sample)
        s = filter_sample(sample["id"], pipeline_version)
        if s:
            yield s


def all_samples(query=None, config=config):
    j = []
    for batch in get_batches(config)["items"]:
        batch_id = batch["sample_batch_id"]
        if batch_id != 266102641558548155500056123446365829360:
            continue
        print(batch_id)
        j += get_samples(batch_id, query=query, config=config)["samples"]
    return j


def submit_batch(batch, apex_token, config=config):
    url = f"{config.host}/batches"
    headers = {
        "Authorization": f"Bearer {apex_token}",
        "Content-type": "application/json",
    }
    response = requests.post(url, headers=headers, json=batch)
    return response


def get_batch_by_name(batch_name, apex_token, config=config):
    print(batch_name, apex_token)
    batches = get_batches(apex_token)
    if isinstance(batches.keys(), KeysView):
        found = False
        for batch in batches['items']:
            if batch_name == batch['fileName']:
                found = True
                print(f"found {batch_name} - batch = {batch}")
                batch_samples = get_batch_samples(batch['sampleBatchId'], apex_token)
                print(f"batch_samples {batch_samples}")
                sample_dict = {}
                for batch_sample in batch_samples['samples']:
                    sample_info = get_sample(batch_sample['id'], apex_token)
                    print(f"sample_info {sample_info}")
                    sample_dict[batch_sample['name']] = sample_info[0]
                    sample_dict[batch_sample['name']]['batchFileName'] = batch['fileName']
                print(f"sample_dict {sample_dict}")
                return sample_dict
        if not found:
            return {}
    else:
        logging.error(f"API response not as expected, quiting. Output - {batches}")
        return {}

def get_analysis(sample_id, apex_token, config=config):
    url = f"{config.host}/samples/{sample_id}"
    method = "GET"
    headers = {"Authorization": f"Bearer {apex_token}"}
    response = requests.get(url, headers=headers)
    #   assert print(response) # is 200
    # print(f"{response}, {response.text}")
    safe_response = response.text.replace(
        "Pipeline Description", '"Pipeline Description"'
    )
    try:
        j = json.loads(safe_response)
    except:
        logging.error(f"empty response from host: {url}")
        return None
    analyses = []
    for sample in j:
        if "analysis" in sample:
            analyses.append(sample["analysis"])
    return analyses


def get_samples(batch_id, apex_token, query=None, negate_query=False, config=config):
    url = f"{config.host}/batches/{batch_id}"
    method = "GET"
    headers = {"Authorization": f"Bearer {apex_token}"}
    response = requests.get(url, headers=headers)
    j = None
    try:
        j = response.json()
    except:
        logging.error("empty response from host")
        return []

    if query:
        samples = []
        k, v = query
        for sample in j["samples"]:
            match = not negate_query
            if "analysis" not in sample:
                continue
            for analysis in sample["analysis"]:
                if k in analysis:
                    if analysis[k] == v:
                        match = negate_query
            if match:
                samples.append(sample)
        return samples

    else:
        return j

def post_metadata_to_apex(new_dir, data, apex_token):
    # logging.info(apex_token)
    batch_response = requests.post(
        f"{config.host}/batches",
        headers={"Authorization": f"Bearer {apex_token}"},
        json=data,
    )

    try:
        apex_batch = batch_response.json()
    except:
        logging.error(f"apex response was not json: {batch_response.text}")
        logging.error(f"submitted data: {data}")
        return None, None

    batch_id = apex_batch.get("id")
    if not batch_id:
        logging.error(f"failed to get batch id: {data}")
        logging.error(f"apex returned: {apex_batch}")
        return None, None
    print(batch_id)

    samples_response = requests.get(
        f"{config.host}/batches/{batch_id}",
        headers={"Authorization": f"Bearer {apex_token}"},
    )
    apex_samples = samples_response.json()

    return apex_batch, apex_samples
