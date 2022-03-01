#!/env/bin/python3

doc = """
watches a directory which contains COVID ENA uploaded data and 
batches them to be run by catsgo
"""

import csv
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

mydb2 = myclient["dir_watcher"]
dirwatcher_metadata = mydb2["metadata"]

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def get_cached_dirlist(sample_method, path):
    """
    get the list of Samples that have already run from a sample_method (illumina or nanopore) and path (prefix/shard)
    """
    return list(
        dirlist.find({"sample_method": sample_method, "path": path}, {"samples": 1})
    )


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


def create_batch(
    exisiting_dirs, size_batch, new_dirs=None, new_dir_prefix=None, sample_method=None
):
    if new_dirs:
        while len(exisiting_dirs) < size_batch and len(new_dirs) > 0:
            dir = new_dirs.pop()
            metadata = utils.get_ena_metadata(dir)
            validSample = True

            if metadata:
                # Paired fastq for Illumina, single fastq for nanopore
                if sample_method:
                    if (
                        sample_method == "illumina"
                        and len(set((new_dir_prefix / dir).glob("*.fastq.md5"))) != 2
                    ):
                        print(
                            f"{dir} is not a valid illumina sample, only one on the fastqs is avalaible."
                        )
                        validSample = False
                    elif (
                        sample_method == "nanopore"
                        and len(set((new_dir_prefix / dir).glob("*.fastq.md5"))) != 1
                    ):
                        print(
                            f"{dir} is not a valid nanopore sample, there is more than one fastq."
                        )
                        validSample = False
                elif "instrument_platform" in metadata:
                    if (
                        str(metadata["instrument_platform"]).lower() == "illumina"
                        and len(set((new_dir_prefix / dir).glob("*.fastq.gz"))) != 2
                    ):
                        print(
                            f"{dir} is not a valid illumina sample, only one on the fastqs is avalaible."
                        )
                        validSample = False
                    elif (
                        sample_method == "nanopore"
                        and len(set((new_dir_prefix / dir).glob("*.fastq.md5"))) != 1
                    ):
                        print(
                            f"{dir} is not a valid nanopore sample, there is more than one fastq."
                        )
                        validSample = False
                else:
                    print(f"Cannot determine sample method for {dir}.")
                    validSample = False

                if validSample:
                    # Check md5 sums of sequences against ENA values
                    for file in (new_dir_prefix / dir).glob("*.fastq.md5"):
                        # get the md5 from the file
                        if not file.with_suffix(".md5").exists():
                            print(f"md5 checksum does not exist for {file}")
                            break
                        with file.with_suffix(".md5").open(mode="rb") as md5_file:
                            ena_md5 = md5_file.read()
                        validFile = False
                        seq_md5 = ""
                        with file.open(mode="rb") as seq_file:
                            md5_hash = hashlib.md5()
                            content = seq_file.read()
                            md5_hash.update(content)
                            seq_md5 = md5_hash.hexdigest()
                        validFile = seq_md5 == ena_md5
                        # for ena_md5 in metadata["fastq_md5"].split(";"):
                        #     if ena_md5 == seq_md5:
                        #         validFile = True
                        #         break
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

        # No new dirs, return working lists
        return (exisiting_dirs, new_dirs)
    else:
        return (exisiting_dirs, [])


def process_batch(sample_method, samples_to_submit, batch_dir):
    print(f"processing {samples_to_submit}")
    samples = list()
    sample_shards = dict()
    batch_name = "ENA-" + str(uuid.uuid4())[:7]
    submission_name = f"Entry for ENA sample processing - {batch_name}"

    for sample, ena_metadata in samples_to_submit:
        # ena_metadata = get_ena_metadata(sample.name)
        p = {
            "name": sample.name,
            "tags": ["ENA_Data"],
            "submissionTitle": submission_name,
            "submissionDescription": submission_name,
            # "specimenOrganism" : ena_metadata["scientific_name"],
            "status": "Uploaded",
            "instrument": {
                "platform": ena_metadata["instrument_platform"],
                "model": ena_metadata["instrument_model"],
                "flowcell": 0,
            },
        }

        if ena_metadata["collection_date"] != "":
            p["collectionDate"] = ena_metadata["collection_date"]
        elif ena_metadata["first_public"]:
            p["collectionDate"] = ena_metadata["first_public"]

        # if ena_metadata["country"] != "":
        p["country"] = ena_metadata["country"]
        # else:
        # p["country"] = "United Kingdom"

        if (
            ena_metadata["scientific_name"]
            == "Severe acute respiratory syndrome coronavirus 2"
        ):
            p["specimenOrganism"] = "SARS-CoV-2"
        else:
            p["specimenOrganism"] = ena_metadata["scientific_name"]

        if sample_method.name == "illumina":
            p["peReads"] = [
                {
                    "r1_sp3_filepath": str(
                        Path(sample) / (sample.name + "_1.fastq.gz")
                    ),
                    "r2_sp3_filepath": str(
                        Path(sample) / (sample.name + "_2.fastq.gz")
                    ),
                }
            ]
            p["seReads"] = []
        elif sample_method.name == "nanopore":
            p["seReads"] = [
                {
                    "sp3_filepath": str(Path(sample) / (sample.name + "_1.fastq.gz")),
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
            "bucketName": sample_method.parent.name,
            "organisation": "Public Repository Data",
            "site": "ENA Data",
            "uploadedOn": datetime.datetime.now().isoformat()[:-3] + "Z",
            # "uploadedBy": "Jeremy.Swann@ndm.ox.ac.uk",
            "uploadedBy": "MARC.BROUARD@NDM.OX.AC.UK",
            "samples": samples,
        }
    }

    apex_token = db.get_apex_token()
    apex_batch, apex_samples = db.post_metadata_to_apex(submission, apex_token)

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
                    sample.relative_to(
                        Path("/data/inputs/s3/") / submission["batch"]["bucketName"]
                    )
                )
                + "/",
                "sample_accession": sample.name,
            }
            writer1.writerow(out)

    ret = catsgo.run_covid_ena(
        f"oxforduni-ncov2019-artic-nf-{sample_method.name}",
        str(ena_batch_csv),
        batch_name,
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


def watch(watch_dir="", batch_dir="", size_batch=200):
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
                            for w in (
                                watch_dir / sample_method / prefix_dir / shard_dir
                            ).glob("*")
                            if w.is_dir()
                        ]
                    ):
                        # Get all sample directories (each of which should only have one sample!)
                        candidate_dirs = set(
                            [
                                z.name
                                for z in (
                                    watch_dir
                                    / sample_method
                                    / prefix_dir
                                    / shard_dir
                                    / sub_shard_dir
                                ).glob("*")
                                if z.is_dir()
                                and len(
                                    set(
                                        (
                                            watch_dir
                                            / sample_method
                                            / prefix_dir
                                            / shard_dir
                                            / sub_shard_dir
                                            / z
                                        ).glob("*")
                                    )
                                )
                                >= 1
                            ]
                        )
                        # get directories/submissions that have already been processed
                        cached_dirlist = set(
                            get_cached_dirlist(
                                sample_method.name,
                                str(Path(prefix_dir) / shard_dir / sub_shard_dir),
                            )
                        )
                        # submissions to be processed are those that are new and have not been marked as failed
                        new_dirs = candidate_dirs.difference(cached_dirlist)
                        # print(f"{sample_method} - {prefix_dir} - {shard_dir} - {new_dirs}")

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
                                )

                                # Check if submitting
                                if len(samples_to_submit) >= size_batch:
                                    samples_to_submit = process_batch(
                                        sample_method, samples_to_submit, batch_dir
                                    )
            # Should submit leftovers for this sample_method to avoid mixing.
            samples_to_submit = process_batch(
                sample_method, samples_to_submit, batch_dir
            )

        print("sleeping for 60")
        time.sleep(60)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    argh.dispatch_commands(
        [
            watch,
        ]
    )
