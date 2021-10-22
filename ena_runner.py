#!/env/bin/python3

doc = """
watches a directory which contains COVID ENA uploaded data and 
batches them to be run by catsgo
"""

import logging
import time
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

# def get_cached_dirlist(sample_method, path):
#     """
#     get the list of Samples that have already run from a sample_method (illumina or nanopore) and path (prefix/shard)
#     """
#     ret = dirlist.find({"sample_method": sample_method}, {"path": path}, {"samples": 1})
#     if not ret:
#         return list()
#     else:
#         return list(ret.get("samples"))

def process_batch(samples_to_submit):
    print(f"processing {samples_to_submit}")
    return []

def watch(
    watch_dir="",
    bucket_name="",
    batch_size=10
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
            samples_to_submit = []
            # These are the prefixs of the sample asscessions (E.G. ERR408 has, in shards (000-010), samples ERR4080000 - ERR4089999)
            for prefix_dir in set([x.name for x in sample_method.glob("*") if x.is_dir()]):
                # Go through each shard seperately
                for shard_dir in set([y.name for y in ( watch_dir / sample_method / prefix_dir ).glob("*") if y.is_dir()]):
                    # Get all sample directories (each of which should only have one sample!)
                    candidate_dirs = set([z.name for z in ( watch_dir / sample_method / prefix_dir / shard_dir ).glob("*") if z.is_dir() and len(set(( watch_dir / sample_method / prefix_dir / shard_dir / z ).glob("*"))) == 2 ])
                    # get directories/submissions that have already been processed
                    #cached_dirlist = set(get_cached_dirlist(sample_method, str( prefix_dir / shard_dir)))
                    # submissions to be processed are those that are new and have not beek marked as failed
                    #new_dirs = candidate_dirs.difference(cached_dirlist)
                    new_dirs = candidate_dirs
                    #print(f"{sample_method} - {prefix_dir} - {shard_dir} - {new_dirs}")

                    if new_dirs:
                        new_dirs = list(new_dirs)
                        print(f"new_dirs - {new_dirs}")
                        # Check if submitting
                        while len(new_dirs) + len(samples_to_submit) >= batch_size:
                            print(f"adding {[ z for z in new_dirs[:batch_size - len(samples_to_submit)]]}")
                            samples_to_submit += [watch_dir / sample_method / prefix_dir / shard_dir / z for z in new_dirs[:batch_size - len(samples_to_submit)]]
                            samples_to_submit = process_batch(samples_to_submit)
                            new_dirs = new_dirs[batch_size - len(samples_to_submit):]
                            print(f"modified new_dirs - {new_dirs}")
                        samples_to_submit += [watch_dir / sample_method / prefix_dir / shard_dir / z for z in new_dirs]
                        print(f"sample_to_submit after {sample_method}/{prefix_dir}/{shard_dir} - {samples_to_submit}")
            samples_to_submit = process_batch(samples_to_submit)
            

        print("sleeping for 60")
        time.sleep(60)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    argh.dispatch_commands(
        [
            watch,
        ]
    )
