import requests
import json
import sys
import configparser
from datetime import datetime


class Config:
    token = None
    host = None
    user = None

    def __init__(self, fn):
        c = configparser.ConfigParser()
        c.read_file(open(fn))
        self.token = c["oracle_rest"]["token"]
        self.host = c["oracle_rest"]["host"]
        self.user = c["oracle_rest"]["user"]


config = Config("config.ini")


def update_sample(sample_id, data, config=config):
    url = f"{config.host}/samples/{sample_id}"
    headers = {
        "Authorization": f"Bearer {config.token}",
        "Content-type": "application/json",
    }
    response = requests.put(url, headers=headers, data=data)
    return response


def get_batches(config=config):
    url = f"{config.host}/batches"
    method = "GET"
    headers = {"Authorization": f"Bearer {config.token}"}
    response = requests.get(url, headers=headers)
    try:
        return response.json()
    except:
        print("empty response from host", file=sys.stderr)
        return []


def get_sample(sample_id, config=config):
    url = f"{config.host}/samples/{sample_id}"
    method = "GET"
    headers = {"Authorization": f"Bearer {config.token}"}
    response = requests.get(url, headers=headers)
    #   assert print(response) # is 200
    print(response)
    print(response.text)
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


def submit_batch(batch, config=config):
    url = f"{config.host}/batches"
    headers = {
        "Authorization": f"Bearer {config.token}",
        "Content-type": "application/json",
    }
    response = requests.post(url, headers=headers, json=batch)
    return response


def batch_from_csv(fn):
    fd = open(fn)
    header = fd.readline().strip().split(",")
    time = datetime.now().isoformat(timespec="milliseconds")
    batch = {
        "fileName": fn,
        "uploadedOn": f"{time}Z",
        "uploadedBy": config.user,
        "organisation": "PHE",
        "site": "PHE OUH",
        "errorMsg": "",
        "samples": [],
    }

    for line in fd:
        r = dict(zip(header, line.strip().split(",")))
        if not r["object_store_bucket"]:
            continue
        collection_date = datetime.fromisoformat(r["sample_date"]).isoformat(
            timespec="milliseconds"
        )
        batch["samples"].append(
            {
                "name": r["experiment_accession"],
                "host": "Homo sapiens",
                "collectionDate": f"{collection_date}Z",
                "country": r["ena_country"],
                "fileName": r["submitted_ftp"],
                "specimenOrganism": "SARS-CoV-2",
                "specimenSource": "swab",
                "status": "Uploaded",
                "instrument": {
                    "platform": r["instrument_platform"],
                    "model": "test",
                    "flowcell": "96",
                },
            }
        )
    return {"batch": batch}


def get_analysis(sample_id, config=config):
    url = f"{config.host}/samples/{sample_id}"
    method = "GET"
    headers = {"Authorization": f"Bearer {config.token}"}
    response = requests.get(url, headers=headers)
    #   assert print(response) # is 200
    print(f"{response}, {response.text}")
    safe_response = response.text.replace(
        "Pipeline Description", '"Pipeline Description"'
    )
    try:
        j = json.loads(safe_response)
    except:
        print(f"empty response from host: {url}", file=sys.stderr)
        return None
    if "analysis" in j:
        return j["analysis"]
    return []


def get_samples(batch_id, query=None, negate_query=False, config=config):
    url = f"{config.host}/batches/{batch_id}"
    method = "GET"
    headers = {"Authorization": f"Bearer {config.token}"}
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
