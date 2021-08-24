import requests
import json
import sys
import configparser
from datetime import datetime


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
        print("empty response from host", file=sys.stderr)
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
        print(f"empty response from host: {url}", file=sys.stderr)
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
        print("empty response from host", file=sys.stderr)
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
