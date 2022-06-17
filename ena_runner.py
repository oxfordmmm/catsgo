#!/env/bin/python3

doc = """
watches a directory which contains COVID ENA uploaded data and 
batches them to be run by catsgo
"""

import csv
import gzip
import logging
import time
import uuid
import json
import datetime
from pathlib import Path
import hashlib
import sys

import argh
import pymongo
import requests

import db
import catsgo
import utils

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["ena_runner"]
dirlist = mydb["dirlist"]
ignore_list = mydb["ignore_list"]

mydb2 = myclient["dir_watcher"]
dirwatcher_metadata = mydb2["metadata"]

config = utils.load_config("config.json")

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def get_cached_dirlist(sample_method, path):
    """
    get the list of Samples that have already run from a sample_method (illumina or nanopore) and path (prefix/shard)
    """
    ret = dirlist.find_one(
        {"sample_method": sample_method, "path": path}, {"samples": 1}
    )
    if not ret:
        return list()
    else:
        return list(ret.get("samples"))
    # return list(
    #    dirlist.find({"sample_method": sample_method, "path": path}, {"samples": 1})
    # )


def add_to_cached_dirlist(sample_method, path, samples):
    """
    add the samples to the list of samples that have batched
    """

    logging.debug(f"adding {samples} to {sample_method}, {path}")
    dirlist.update_one(
        {"sample_method": sample_method, "path": path},
        {"$push": {"samples": {"$each": samples}}},
        upsert=True,
    )


def get_md5_file_hash(file_path):
    with open(file_path, "rb") as f:
        bytes = f.read()
        return hashlib.md5(bytes).hexdigest()

def get_ignore_list(sample_method, path):
    r = ignore_list.find_one({"sample_method": sample_method, "path": path}, {"ignore_list": 1})
    if r:
        return r.get("ignore_list", list())
    else:
        return list()

def add_to_ignore_list(sample_method, path, samples):
    ignore_list.update_one(
        {"sample_method": sample_method, "path": path},
        {"$push": {"ignore_list": samples}},
        upsert=True,
    )

def create_batch(
    exisiting_dirs, size_batch, new_dirs=None, new_dir_prefix=None, sample_method=None, watch_dir=None,
):
    if new_dirs:
        while len(exisiting_dirs) < size_batch and len(new_dirs) > 0:
            dir = new_dirs.pop()
            metadata = utils.get_ena_metadata_from_csv(dir, watch_dir)
            validSample = True

            if metadata:
                # Paired fastq for Illumina, single fastq for nanopore
                if sample_method or "instrument_platform" in metadata:
                    
                    # ILLUMINA
                    if (sample_method == "illumina") or str(metadata["instrument_platform"]).lower() == "illumina":
                        # check both fastqs are avalaible
                        if len(set((new_dir_prefix / dir).glob("*.fastq.md5"))) != 2:
                            print(f"{dir} is not a valid illumina sample, only one on the fastqs is avalaible.")
                            validSample = False

                    # NANOPORE
                    elif (sample_method == "nanopore") or str(metadata["instrument_platform"]).lower() == "nanopore":
                        # check that only one fastq exists
                        if len(set((new_dir_prefix / dir).glob("*.fastq.md5"))) != 1:
                            print(f"{dir} is not a valid nanopore sample, there is more than one fastq.")
                            validSample = False
 
                else:
                    print(f"Cannot determine sample method for {dir}.")
                    validSample = False

                if not metadata["collection_date_ok"]:
                    validSample = False

                if validSample:
                    # Check md5 sums of sequences against ENA values stored in the .md5 files
                    # this is done because read-it-and-keep has been run across the files
                    for file in (new_dir_prefix / dir).glob("*.fastq.gz"):
                        # get the md5 from the file
                        if not file.with_suffix(".md5").exists():
                            print(f"md5 checksum does not exist for {file}")
                            break
                        with file.with_suffix(".md5").open(mode="rb") as md5_file:
                            seq_md5 = md5_file.read().decode("utf8")
                        validFile = False
                        for ena_md5 in metadata["fastq_md5"].split(";"):
                            if ena_md5 == seq_md5:
                                validFile = True
                                break
                        if not validFile:
                            print(
                                f"{dir} is not a valid sample, an md5 does not match the ena md5."
                            )
                            print(f"seq md5 = {seq_md5}")
                            print(f"ena md5 = {metadata['fastq_md5']}")
                            validSample = False
                            break

                    if validSample:
                        exisiting_dirs.append((new_dir_prefix / dir, metadata))
            else:
                print(f"Cannot get ENA metadata for {dir}.")
                validSample = False
            
            if not validSample:
                logging.info(f"Sample {dir} is not valid, skipping and adding to ignore list")
                # horrible hack to get the path
                path = new_dir_prefix.parent.parent.name + "/" + new_dir_prefix.parent.name + "/" + new_dir_prefix.name
                # add the sample to the completion list, so that it is ignored in future
                add_to_ignore_list(sample_method, str(path), dir)

        # No new dirs, return working lists
        return (exisiting_dirs, new_dirs)
    else:
        return (exisiting_dirs, [])


def process_batch(sample_method, samples_to_submit, batch_dir, workflow):
    print(f"processing {samples_to_submit}")
    samples = list()
    sample_shards = dict()
    batch_name = "ENA-" + str(uuid.uuid4())[:7]
    submission_name = f"Entry for ENA sample processing - {batch_name}"

    for sample, ena_metadata in samples_to_submit:
        p = {
            "name": sample.name,
            "tags": ["ENA_Data"],
            "submissionTitle": submission_name,
            "submissionDescription": submission_name,
            "control": ena_metadata["control"],
            "collection_date": ena_metadata["collection_date"],
            "status": "Uploaded",
            "country": ena_metadata["country"],
            "region": ena_metadata["region"],
            "district": ena_metadata["district"],
            "specimen": ena_metadata["specimen_organism"],
            "host": ena_metadata["host"],
            "instrument": {"platform": ena_metadata["instrument_platform"],},
            "primer_scheme": ena_metadata["primer_scheme"],
        }
    
        if sample_method.name == "illumina":
            p["peReads"] = [
                {
                    "r1_uri": str(Path(sample) / (sample.name + ".reads_1.fastq.gz")),
                    "r1_md5": get_md5_file_hash(str(Path(sample) / (sample.name + ".reads_1.fastq.gz"))),
                    "r2_uri": str(Path(sample) / (sample.name + ".reads_2.fastq.gz")),
                    "r2_md5": get_md5_file_hash(str(Path(sample) / (sample.name + ".reads_2.fastq.gz"))),
                }
            ]
            p["seReads"] = []
        elif sample_method.name == "nanopore":
            p["seReads"] = [
                {
                    "uri": str(Path(sample) / (sample.name + ".reads.fastq.gz")),
                    "md5": get_md5_file_hash(str(Path(sample) / (sample.name + ".reads.fastq.gz"))),
                }
            ]
            p["peReads"] = []
        else:
            logging.error(f"Invalid sample_method {sample_method}")
        samples.append(p)
        # Add to dict for submitting to cached_dirs
        path = sample.relative_to(sample_method)
        path = str(path.parent)
        if path in sample_shards:
            sample_shards[path].append(sample.name)
        else:
            sample_shards[path] = [sample.name]

    submission = {
        "batch": {
            "fileName": batch_name,
            "bucketName": sample_method.parent.parent.name,
            "organisation": "Public Repository Data",
            "site": "ENA Data",
            "uploadedOn": datetime.datetime.now().isoformat()[:-3] + "Z",
            # "uploadedBy": "Jeremy.Swann@ndm.ox.ac.uk",
            "uploadedBy": config["ENA_user"],
            "samples": samples,
        }
    }

    apex_token = db.get_apex_token()
    apex_batch, apex_samples = db.post_metadata_to_apex(submission, apex_token)
    upload_bucket = db.get_output_bucket_from_input(
        sample_method.parent.parent.name, apex_token
    )

    for path, sample_list in sample_shards.items():
        add_to_cached_dirlist(sample_method.name, path, sample_list)

    # Add to batch_dir
    ena_batch_csv = Path(batch_dir) / f"{batch_name}.csv"
    out_fieldnames = ["bucket", "sample_prefix", "sample_accession"]
    with open(ena_batch_csv, "w") as out_csv:
        writer1 = csv.DictWriter(out_csv, fieldnames=out_fieldnames)
        writer1.writeheader()
        for sample, ena_metadata in samples_to_submit:
            out = {
                "bucket": submission["batch"]["bucketName"],
                "sample_prefix": str(
                    sample.relative_to(Path("/data/inputs/s3/") / submission["batch"]["bucketName"])
                )
                + "/",
                "sample_accession": sample.name,
            }
            writer1.writerow(out)
    
    if str(workflow).lower() == "sars-cov2_workflows":
        flow = f"oxforduni-gpas-sars-cov2-{sample_method.name}"
    else:
        flow = f"oxforduni-ncov2019-artic-nf-{sample_method.name}"

    ret = catsgo.run_covid_ena(
        flow,
        str(ena_batch_csv),
        batch_name,
        upload_bucket,
    )

    dirwatcher_metadata.update_one(
        {"catsup_uuid": batch_name},
        {
            "$set": {
                "run_uuid": ret.get("run_uuid", ""),
                "added_time": str(int(time.time())),
                "apex_batch": apex_batch,
                "apex_samples": apex_samples,
                "submitted_metadata": samples,
            }
        },
        upsert=True,
    )

    return []


def watch(watch_dir="", batch_dir="", size_batch=200, flow="ncov2019-artic-nf"):
    """ """
    print(doc)
    watch_dir = Path(watch_dir)
    if not watch_dir.is_dir():
        logging.error(f"{watch_dir} is not a directory")
        sys.exit(1)

    while True:
        # ENA bucket will have illumina and nanopore data
        for sample_method in [watch_dir / "illumina", watch_dir / "nanopore"]:
            samples_to_submit = []
            # These are the prefixs of the sample asscessions (E.G. ERR408 has, in shards (000-010), samples ERR4080000 - ERR4089999)
            for prefix_dir in set(
                [x.name for x in sample_method.glob("*") if x.is_dir()]
            ):
                # Go through each shard seperately
                for shard_dir in set(
                    [
                        y.name
                        for y in (watch_dir / sample_method / prefix_dir).glob("*")
                        if y.is_dir()
                    ]
                ):
                    for sub_shard_dir in set(
                        [
                            w.name
                            for w in (watch_dir / sample_method / prefix_dir / shard_dir).glob("*")
                            if w.is_dir()
                        ]
                    ):
                        # Get all sample directories (each of which should only have one sample!)
                        candidate_dirs = set(
                            [
                                z.name
                                for z in (watch_dir / sample_method / prefix_dir / shard_dir / sub_shard_dir).glob("*")
                                if z.is_dir()
                                and len(set((watch_dir / sample_method / prefix_dir / shard_dir / sub_shard_dir / z).glob("*.fastq.gz"))) >= 1
                            ]
                        )
                        # get directories/submissions that have already been processed
                        cached_dirlist = set(
                            get_cached_dirlist(
                                sample_method.name,
                                str(Path(prefix_dir) / shard_dir / sub_shard_dir),
                            )
                        )
                        # submissions to be processed are those that are new and have not been marked as failed or finished
                        bad_submission_uuids = set(
                            get_ignore_list(sample_method.name, 
                            str(Path(prefix_dir) / shard_dir / sub_shard_dir))
                        )
                        logging.debug(f"{bad_submission_uuids} are being ignored as they have previously been detected as invalid")
                        new_dirs = candidate_dirs.difference(cached_dirlist)
                        new_dirs = new_dirs.difference(bad_submission_uuids)
                        

                        if new_dirs:
                            new_dirs = list(new_dirs)
                            while len(new_dirs) > 0:
                                # Assemble batch after checking No. of files and md5 sums are correct
                                (samples_to_submit, new_dirs) = create_batch(
                                    samples_to_submit,
                                    size_batch,
                                    new_dirs,
                                    watch_dir
                                    / sample_method
                                    / prefix_dir
                                    / shard_dir
                                    / sub_shard_dir,
                                    sample_method.name,
                                    watch_dir,
                                )

                                # Check if submitting
                                if len(samples_to_submit) >= size_batch:
                                    samples_to_submit = process_batch(
                                        sample_method, samples_to_submit, batch_dir, flow
                                    )
                                    print(f'sleeping for {config["ENA_sleep_time"]}')
                                    time.sleep(int(config["ENA_sleep_time"]))
            # Should submit leftovers for this sample_method to avoid mixing.
            if len(samples_to_submit) >= 1:
                samples_to_submit = process_batch(
                    sample_method, samples_to_submit, batch_dir, flow
                )
                print(f'sleeping for {config["ENA_sleep_time"]}')
                time.sleep(int(config["ENA_sleep_time"]))
        print("sleeping for 60")
        time.sleep(60)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    argh.dispatch_commands(
        [
            watch,
        ]
    )
