#!/env/bin/python3

doc = """
watches a directory which contains COVID ENA uploaded data and 
batches them to be run by catsgo
"""

import logging
from pathlib import Path

import argh
import pymongo

import db

# myclient = pymongo.MongoClient("mongodb://localhost:27017/")
# mydb = myclient["ena_runner"]
# dirlist = mydb["dirlist"]

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# def get_cached_dirlist(path):
#     """
#     get the list of Samples that have already run from a path (prefix/shard)
#     """
#     ret = dirlist.find({"path": path}, {"samples": 1})
#     if not ret:
#         return list()
#     else:
#         return list(ret.get("samples"))

def watch(
    watch_dir="",
    bucket_name="",
):
    """
    
    """
    print(doc)
    watch_dir = Path(watch_dir)
    if not watch_dir.is_dir():
        logging.error(f"{watch_dir} is not a directory")
        sys.exit(1)

    while True:
        # Illumina/ Nanopore
        # for dir in ERR***
        # for dir in 000-010
        # for dir in ERR*******
        # samples!!!
        # ENA bucket will have illumina and nanopore data
        for sample_method in [ watch_dir / "illumina", watch_dir / "nanopore"]:
            # These are the prefixs of the sample asscessions (E.G. ERR408 has, in shards (000-010), samples ERR4080000 - ERR4089999)
            for prefix_dir in set([x.name for x in sample_method.glob("*") if x.is_dir()]):
                # Go through each shard seperately
                for shard_dir in set([x.name for x in Path(prefix_dir).glob("*") if x.is_dir()]):
                    # Get all sample directories (each of which should only have one sample!)
                    candidate_dirs = set([x.name for x in Path(shard_dir).glob("*") if x.is_dir() and len(x.glob("*")) == 2 ])
                    # get directories/submissions that have already been processed
                    #cached_dirlist = set(get_cached_dirlist(str(shard_dir)))
                    # submissions to be processed are those that are new and have not beek marked as failed
                    #new_dirs = candidate_dirs.difference(cached_dirlist)
                    print(f"{shard_dir} - {candidate_dirs}")

                    # if new_dirs:
                    #     apex_token = db.get_apex_token()
                    # for new_dir in new_dirs:  #  new_dir is the catsup upload uuid
                    #     r = process_dir(
                    #         new_dir, watch_dir, bucket_name, apex_token, max_submission_attempts
                    #     )
                    #     if r:
                    #         # if we've started a run then stop processing and go to sleep. This prevents
                    #         # the system from being overwhelmed with nextflow starting
                    #         break

        print("sleeping for 60")
        time.sleep(60)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    argh.dispatch_commands(
        [
            watch,
        ]
    )
