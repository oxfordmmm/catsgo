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
import db

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["dir_watcher"]
dirlist = mydb["dirlist"]
metadata = mydb["metadata"]
ignore_list = mydb["ignore_list"]

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


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


def get_ignore_list(watch_dir):
    r = ignore_list.find_one({"watch_dir": watch_dir}, {"ignore_list": 1})
    if r:
        return r.get("ignore_list", list())
    else:
        return list()


def add_to_ignore_list(watch_dir, submission_uuid):
    ignore_list.update_one(
        {"watch_dir": watch_dir},
        {"$push": {"ignore_list": submission_uuid}},
        upsert=True,
    )


def remove_from_cached_dirlist(watch_dir, new_dir):
    logging.debug(f"removing {new_dir}")
    dirlist.update_one(
        {"watch_dir": watch_dir}, {"$pull": {"dirs": new_dir}}, upsert=True
    )


def which_pipeline_csv(watch_dir, new_dir):
    rows = json.loads(get_and_format_metadata(watch_dir, new_dir))
    for sample in rows.get("batch", dict()).get("samples", list()):
        instrument = sample.get("instrument", dict())
        platform = instrument.get("platform", str())
        model = instrument.get("model", str())
        platform_lower_words = [word.lower() for word in platform.split()]
        model_lower_words = [word.lower() for word in platform.split()]
        if "nanopore" in platform_lower_words:
            return "nanopore-1"
        if "nanopore" in model_lower_words:
            return "nanopore-1"
        if "illumina" in platform_lower_words:
            return "illumina-1"
        if "illumina" in model_lower_words:
            return "illumina-1"

    # default illumina
    return "illumina-1"

def which_pipeline_db(watch_dir, new_dir, metadata_dict = None):
    for sample in metadata_dict['samples'].values():
        platform = sample['instrumentPlatform']
        platform_lower_words = [word.lower() for word in platform.split()]
        if "nanopore" in platform_lower_words:
            return "nanopore-1"
        if "illumina" in platform_lower_words:
            return "illumina-1"

    # default illumina
    return "illumina-1"

def get_and_format_metadata(watch_dir, new_dir):
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
            "bucketName": Path(watch_dir).name,
            "uploadedOn": uploadedOn,
            "uploadedBy": row.get("submitter_email", ""),
            "organisation": row.get("submitter_organisation", ""),
            "site": row.get("submitter_site", ""),
            "errorMsg": "",
        }
        p = {
            "name": row.get("sample_uuid4", ""),
            "host": row.get("sample_host", ""),
            "collectionDate": uploadedOn,  # TODO row.get("sample_collection_date", ""),
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


submission_attempts = defaultdict(int)


def process_dir(new_dir, watch_dir, bucket_name, apex_token, max_submission_attempts):
    """
    the watch process has detected a new upload. this processes it

    new_dir is the catsup upload directory, which is also the upload uuid
    """
    if not (Path(watch_dir) / new_dir).is_dir():
        logging.warning(f"dir_watcher: {new_dir}: expected a directory, found a file")
        return
    if not (Path(watch_dir) / new_dir / "upload_done.txt").is_file():
        # at the end of the upload, the client uploads an empty file upload_done.txt. This is how we know that the upload has finished and we are ready to run the pipeline on it
        logging.info(f"dir_watcher: {new_dir} upload in progress?")
        return

    if submission_attempts[new_dir] >= max_submission_attempts:
        logging.warning(f"bad submission: {new_dir}")
        add_to_ignore_list(str(watch_dir), new_dir)
        return

    submission_attempts[new_dir] += 1
    logging.info(f"attempt {submission_attempts[new_dir]}")

    pipelines = ["illumina-1", "nanopore-1"]
    pipeline = pipelines[0]
    apex_batch = {}
    apex_samples = {}
    data = {}
    try:
        if (Path(watch_dir) / new_dir / "sp3data.csv").is_file():
            with open(Path(watch_dir) / new_dir / "sp3data.csv", 'r') as infile:
                reader = csv.DictReader(infile)
                if len(reader.fieldnames()) < 3:
                    logging.error("Found APEX run {new_dir}, will not attempt to run again.")
                    return False
            
            pipeline = which_pipeline_csv(watch_dir, new_dir)
            if pipeline not in pipelines:
                logging.warning(f"unknown pipeline: {pipeline} not in {pipelines}")

            #        try:
            # submit the pipeline run
            # add to it list of stuff already run
            data_x = get_and_format_metadata(watch_dir, new_dir)
            data = json.loads(data_x)
            # logging.info(data)
            apex_batch, apex_samples = db.post_metadata_to_apex(data, apex_token)
            if not apex_batch:
                return
        else:
            # Get metadata for batch from ORDS DB
            batch_samples = db.get_batch_by_name(new_dir, apex_token)
            if len(batch_samples.keys()) > 0:
                pipeline = which_pipeline_db(watch_dir, new_dir, batch_samples)
                if pipeline not in pipelines:
                    logging.warning(f"unknown pipeline: {pipeline} not in {pipelines}")
                # Write out submission_uuid4, sample_uuid4 to sp3data.csv
                sp3data_csv = Path(watch_dir) / new_dir / "sp3data.csv"
                out_fieldnames = ['submission_uuid4', 'sample_uuid4']
                with open(sp3data_csv, 'w') as out_csv:
                    writer1 = csv.DictWriter(out_csv, fieldnames=out_fieldnames)
                    writer1.writeheader()
                    for sample in batch_samples['samples'].values():
                        out = {
                            'submission_uuid4' : sample['batchFileName'],
                            'sample_uuid4' : sample['name']
                        }
                        writer1.writerow(out)
                apex_batch = batch_samples
                data = batch_samples
                apex_samples = db.get_batch_samples(apex_batch['id'], apex_token)
            else:
                logging.error(f"No sp3data.csv and could not access ORDS DB for {new_dir}.")
                return False

        upload_bucket = db.get_output_bucket_from_input(bucket_name)

        if pipeline == "illumina-1":
            ret = catsgo.run_covid_illumina_catsup(
                "oxforduni-ncov2019-artic-nf-illumina",
                str(Path(watch_dir) / new_dir),
                bucket_name,
                upload_bucket,
                new_dir,
            )
        elif pipeline == "nanopore-1":
            ret = catsgo.run_covid_illumina_catsup(  # it says illumina but the form is the same so we can reuse it here
                "oxforduni-ncov2019-artic-nf-nanopore",
                str(Path(watch_dir) / new_dir),
                bucket_name,
                upload_bucket,
                new_dir,
            )
        else:
            logging.error("unknown pipeline: {pipeline}. This shouldn't be reachable")

        logging.info(ret)
        add_to_cached_dirlist(
            str(watch_dir),
            new_dir,
            ret.get("run_uuid", ""),
            apex_batch,
            apex_samples,
            data,
        )
        return True  # we've restarted a run
    except Exception as e:
        logging.error(f"Error occurred processing {new_dir}.")
        logging.error(e)
        return False


def watch(
    watch_dir="/data/inputs/s3/oracle-test",
    bucket_name="catsup-test",
    max_submission_attempts=3,
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
        sys.exit(1)

    while True:
        # get all directories in bucket
        # note that directories are named after submission uuids, so this is effectively a list of submission uuids
        candidate_dirs = set([x.name for x in watch_dir.glob("*") if x.is_dir()])
        # get directories/submissions that have already been processed
        cached_dirlist = set(get_cached_dirlist(str(watch_dir)))
        # get directories/submissions that have failed
        bad_submission_uuids = set(get_ignore_list(str(watch_dir)))
        # submissions to be processed are those that are new and have not beek marked as failed
        new_dirs = candidate_dirs.difference(cached_dirlist)
        new_dirs = new_dirs.difference(bad_submission_uuids)

        if new_dirs:
            apex_token = db.get_apex_token()
        for new_dir in new_dirs:  #  new_dir is the catsup upload uuid
            r = process_dir(
                new_dir, watch_dir, bucket_name, apex_token, max_submission_attempts
            )
            if r:
                # if we've started a run then stop processing and go to sleep. This prevents
                # the system from being overwhelmed with nextflow starting
                break

        print("sleeping for 60")
        time.sleep(60)

def get_apex_token():
    return db.get_apex_token()

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    argh.dispatch_commands(
        [
            watch,
            remove_from_cached_dirlist,
            get_apex_token,
            process_dir,
            get_and_format_metadata,
            which_pipeline_csv,
            which_pipeline_db,
        ]
    )
