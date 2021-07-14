doc = """
watches a directory and runs a pipeline on any new subdirectories
"""

import logging
import os
import time
from pathlib import Path

import argh
import gridfs
import pymongo

import catsgo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["dir_watcher"]
dirlist = mydb["dirlist"]
metadata = mydb["metadata"]


def get_cached_dirlist(watch_dir):
    ret = dirlist.find_one({"watch_dir": watch_dir}, {"dirs": 1})
    if not ret:
        return list()
    else:
        return list(ret.get("dirs"))


def add_to_cached_dirlist(watch_dir, new_dir, run_uuid):
    logging.debug(f"adding {new_dir}")
    dirlist.update_one(
        {"watch_dir": watch_dir}, {"$push": {"dirs": new_dir}}, upsert=True
    )
    metadata.update_one(
        {"catsup_uuid": new_dir},
        {
            "$set": {
                "run_uuid": run_uuid,
                "added_time": str(int(time.time())),
            }
        },
        upsert=True,
    )


def remove_from_cached_dirlist(watch_dir, new_dir):
    logging.debug(f"removing {new_dir}")
    dirlist.update_one(
        {"watch_dir": watch_dir}, {"$pull": {"dirs": new_dir}}, upsert=True
    )


def process_dir(new_dir, watch_dir, pipeline, flow_name, bucket_name):
    if not (watch_dir / new_dir).is_dir():
        logging.warning(f"dir_watcher: {new_dir}: expected a directory, found a file")
        return
    if not (watch_dir / new_dir / "upload_done.txt").is_file():
        logging.info(f"dir_watcher: {new_dir} upload in progress?")
        return
    if pipeline == "covid_illumina":
        try:
            ret = catsgo.run_covid_illumina_catsup(
                flow_name, str(watch_dir / new_dir), bucket_name, new_dir
            )
            logging.info(ret)
            # add to it list of stuff already run
            add_to_cached_dirlist(str(watch_dir), new_dir, ret.get("run_uuid", ""))
        except Exception as e:
            logging.error(
                f"dir_watcher: pipeline run exception: pipeline: {pipeline}, new_dir: {new_dir}, watch_dir: {str(watch_dir)}, exception: {str(e)}"
            )


def watch(watch_dir, pipeline, flow_name, bucket_name):
    print(doc)
    watch_dir = Path(watch_dir)
    if not watch_dir.is_dir():
        logging.error(f"{watch_dir} is not a directory")
        os.exit(1)

    while True:
        candidate_dirs = set([x.name for x in Path(watch_dir).glob("*") if x.is_dir()])
        cached_dirlist = set(get_cached_dirlist(str(watch_dir)))
        new_dirs = candidate_dirs.difference(cached_dirlist)

        for new_dir in new_dirs:
            process_dir(new_dir, watch_dir, pipeline, flow_name, bucket_name)

        logging.info("sleeping for 60")
        time.sleep(60)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    argh.dispatch_commands([watch, remove_from_cached_dirlist])
