doc = """
watches a directory and runs a pipeline on any new subdirectories
submits metadata to api
"""

import csv
import datetime
import json
import logging
import os
import time
from collections import defaultdict
from pathlib import Path

import argh
import gridfs
import pymongo
import requests

import catsgo
from db import Config

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["dir_watcher"]
dirlist = mydb["dirlist"]
metadata = mydb["metadata"]


def get_cached_dirlist(watch_dir):
    """
    get the list of uuids that have already run

    (since the dirs are named after catsup upload uuids)
    """
    ret = dirlist.find_one({"watch_dir": watch_dir}, {"dirs": 1})
    if not ret:
        return list()
    else:
        return list(ret.get("dirs"))


def add_to_cached_dirlist(
    watch_dir, new_dir, run_uuid, apex_batch, apex_samples, submitted_metadata
):
    """
    add the uuid to the list of uuids that have been run on sp3

    store some metadata as well
    """

    print(apex_batch)
    print(apex_samples)

    apex_batch["id"] = str(apex_batch["id"])  # ugh

    logging.debug(f"adding {new_dir}")
    dirlist.update_one(
        {"watch_dir": watch_dir}, {"$push": {"dirs": new_dir}}, upsert=True
    )
    logging.info(f"{run_uuid}, {submitted_metadata}, {apex_batch}")
    metadata.update_one(
        {"catsup_uuid": new_dir},
        {
            "$set": {
                "run_uuid": run_uuid,
                "added_time": str(int(time.time())),
                "apex_batch": apex_batch,
                "apex_samples": apex_samples,
                "submitted_metadata": submitted_metadata,
            }
        },
        upsert=True,
    )


def remove_from_cached_dirlist(watch_dir, new_dir):
    logging.debug(f"removing {new_dir}")
    dirlist.update_one(
        {"watch_dir": watch_dir}, {"$pull": {"dirs": new_dir}}, upsert=True
    )


def get_and_format_metadata(watch_dir, new_dir):
    config = Config("config.ini")
    data_file = Path(watch_dir) / new_dir / "sp3data.csv"
    logging.info(f"processing {data_file}")
    if not data_file.is_file():
        logging.error(f"get_and_format_metadata: {data_file} not a file")
        return

    with open(data_file, newline="") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        logging.error(f"send_metadata_to_api: {data_file} no rows")
        return

    out = {"batch": {"samples": list()}}

    rows_by_sample = defaultdict(list)
    for row in rows:
        rows_by_sample[row.get("sample_uuid4")].append(row)

    uploadedOn = datetime.datetime.now().isoformat()[:-3] + "Z"

    seen_sample_uuid4s = set()
    for row in rows:
        if row.get("sample_uuid4") in seen_sample_uuid4s:
            continue
        seen_sample_uuid4s.add(row.get("sample_uuid4"))

        metadata = {
            "fileName": row.get("submission_uuid4", ""),
            "uploadedOn": uploadedOn,
            "uploadedBy": config.user,  # row.get("submitter_email", ""),
            "organisation": "PHE",  # row.get("submitter_organisation", ""),
            "site": "PHE OUH",  # row.get("submitter_site", ""),
            "errorMsg": "",
        }
        p = {
            "name": row.get("sample_uuid4", ""),
            "host": row.get("sample_host", ""),
            "collectionDate": uploadedOn,  # row.get("sample_collection_date", ""),
            "country": row.get("sample_country", ""),
            "fileName": row.get("sample_uuid4", ""),
            "specimenOrganism": row.get("sample_organism", "SARS-CoV-2"),
            "specimenSource": row.get("sample_source", "Swab"),
            "status": "Uploaded",
            "comments": "",
            "sampleDetails3": "",
            "submissionTitle": row.get("submission_title", ""),
            "submissionDescription": row.get("submission_description", ""),
            "instrument": {
                "platform": row.get("instrument_platform", ""),
                "model": row.get("instrument_model", ""),
                "flowcell": row.get("instrument_flowcell", ""),
            },
        }
        rows_for_sample = rows_by_sample.get(row.get("sample_uuid4"))
        if len(rows_for_sample) == 1:
            p["seReads"] = [
                {
                    "uri": "",
                    "sp3_filepath": str(
                        Path(watch_dir)
                        / new_dir
                        / rows_for_sample[0].get("sample_filename")
                    ),
                    "md5": rows_for_sample[0].get("clean_file_md5", ""),
                }
            ]
            p["peReads"] = []
        if len(rows_for_sample) == 2:
            p["peReads"] = [
                {
                    "r1_uri": "",
                    "r1_sp3_filepath": str(
                        Path(watch_dir)
                        / new_dir
                        / rows_for_sample[0].get("sample_filename")
                    ),
                    "r1_md5": rows_for_sample[0].get("clean_file_md5", ""),
                    "r2_uri": "",
                    "r2_sp3_filepath": str(
                        Path(watch_dir)
                        / new_dir
                        / rows_for_sample[1].get("sample_filename")
                    ),
                    "r2_md5": rows_for_sample[1].get("clean_file_md5", ""),
                }
            ]
            p["seReads"] = []

        out["batch"]["samples"].append(p)

    for k, v in metadata.items():
        out["batch"][k] = v

    # dump to json in case it's used on the command-line. Python's output uses single quotes, which isn't valid json
    return json.dumps(out)


def get_apex_token():
    with open("secrets.json") as f:
        c = json.load(f)
        client_id = c.get("client_id")
        client_secret = c.get("client_secret")

    access_token_response = requests.post(
        "{config.idcs}",
        data={"grant_type": "client_credentials", "scope": "{config.url}",},
        verify=False,
        allow_redirects=False,
        auth=(client_id, client_secret),
    )
    access_token = access_token_response.json().get("access_token")
    logging.info(f"got apex token {access_token}")
    return access_token


def post_metadata_to_apex(new_dir, data, apex_token):
    logging.info(apex_token)
    batch_response = requests.post(
        "{config.url}/batches",
        headers={"Authorization": f"Bearer {apex_token}"},
        json=data,
    )
    logging.info(f"apex response: {batch_response.text}")
    apex_batch = batch_response.json()

    batch_id = apex_batch.get("id")
    assert batch_id
    print(batch_id)

    samples_response = requests.get(
        f"{config.url}/batches/{batch_id}",
        headers={"Authorization": f"Bearer {apex_token}"},
    )
    apex_samples = samples_response.json()

    return apex_batch, apex_samples


def process_dir(new_dir, watch_dir, pipeline, flow_name, bucket_name, apex_token):
    """
    the watch process has detected a new upload. this processes it

    new_dir is the catsup upload directory, which is also the upload uuid
    """
    if not (watch_dir / new_dir).is_dir():
        logging.warning(f"dir_watcher: {new_dir}: expected a directory, found a file")
        return
    if not (watch_dir / new_dir / "upload_done.txt").is_file():
        # at the end of the upload, the client uploads an empty file upload_done.txt. This is how we know that the upload has finished and we are ready to run the pipeline on it
        logging.info(f"dir_watcher: {new_dir} upload in progress?")
        return
    if pipeline == "covid_illumina":
        #        try:
        # submit the pipeline run
        ret = catsgo.run_covid_illumina_catsup(
            flow_name, str(watch_dir / new_dir), bucket_name, new_dir
        )
        logging.info(ret)
        # add to it list of stuff already run
        data_x = get_and_format_metadata(watch_dir, new_dir)
        data = json.loads(data_x)
        logging.info(data)
        apex_batch, apex_samples = post_metadata_to_apex(new_dir, data, apex_token)
        add_to_cached_dirlist(
            str(watch_dir),
            new_dir,
            ret.get("run_uuid", ""),
            apex_batch,
            apex_samples,
            data,
        )


#        except Exception as e:
#            logging.error(
#                f"dir_watcher: exception: pipeline: {pipeline}, new_dir: {new_dir}, watch_dir: {str(watch_dir)}, exception: {str(e)}"
#            )


def watch(
    #    watch_dir="/data/inputs/s3/oracle-test",
    watch_dir="/data/inputs/users/admin",
    pipeline="covid_illumina",
    flow_name="oxforduni-ncov2019-artic-nf-illumina",
    bucket_name="catsup-test",
):
    """
    watch watch_dir for new directories that have the upload_done.txt file (signaling that an upload was successful)

    watch_dir example: /data/inputs/s3/oracle-test (for the catsup-test bucket. In the future we should probably name the directories the same as the bucket name!
    pipeline: the pipeline that we want to run (currently only "covid_illumina")
    flow_name: the sp3 flow name (currently oxforduni-ncov2019-artic-nf-illumina)
    bucket_name: the bucket name that's mounted in the watch_dir directory (used by the pipeline to fetch the sample files)
    """

    print(doc)
    watch_dir = Path(watch_dir)
    if not watch_dir.is_dir():
        logging.error(f"{watch_dir} is not a directory")
        os.exit(1)

    while True:
        # get all directories in bucket
        # note that directories are named after submission uuids, so this is effectively a list of submission uuids
        candidate_dirs = set([x.name for x in Path(watch_dir).glob("*") if x.is_dir()])
        # get directories that have already been processed
        cached_dirlist = set(get_cached_dirlist(str(watch_dir)))
        # get directories that need to be checked
        new_dirs = candidate_dirs.difference(cached_dirlist)

        if new_dirs:
            apex_token = get_apex_token()
        for new_dir in new_dirs:  #  new_dir is the catsup upload uuid
            process_dir(
                new_dir, watch_dir, pipeline, flow_name, bucket_name, apex_token
            )

        logging.debug("sleeping for 60")
        time.sleep(60)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    argh.dispatch_commands(
        [watch, remove_from_cached_dirlist, get_apex_token, get_and_format_metadata]
    )
