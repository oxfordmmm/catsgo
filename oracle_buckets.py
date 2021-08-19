import logging
import pathlib
import shlex
import subprocess
import time

import argh

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def is_path_mounted(path):
    mount_out = subprocess.check_output("mount").decode().split()
    return str(path) in mount_out


def go(
    bucket_names="dJEoQNYTEzUXmvtfxFjORAdvrSpvFJum,kcdwRhBRFHIrgeMQnriVpEmeoOgSPrcn,jLyCUEpKBxrixFQRyaxhPwhtMpKqpXjP,GUEOIpiGjcpDArjtCixNdsnvAItKbYaH,moALuXyROLzIGcShSsJWIowMQPVcVlTU",
    bucket_mount_path="/data/inputs/s3/",
    s3fs_creds_file="/home/ubuntu/.passwd-s3fs-oracle-test",
    oracle_url="https://lrbvkel2wjot.compat.objectstorage.uk-london-1.oraclecloud.com",
    user_uid="1001",
    user_gid="1001",
):
    bucket_names = bucket_names.split(",")
    bucket_mount_path = pathlib.Path(bucket_mount_path)

    logging.warning("here we go!!")

    for bucket_name in bucket_names:
        bucket_name = bucket_name.strip()
        bucket_path = bucket_mount_path / bucket_name
        if is_path_mounted(bucket_path):
            logging.info(f"path {bucket_path} already mounted")
            continue
        logging.info(f"Bucket {bucket_name} to be mounted in {bucket_path}")
        bucket_path.mkdir(exist_ok=True)
        mount_cmd = f"s3fs {bucket_name} {bucket_path} -o passwd_file={s3fs_creds_file} -o url={oracle_url} -o use_path_request_style -o uid={user_uid},gid={user_gid}"
        logging.info(f"s3fs cmd: {mount_cmd}")
        try:
            subprocess.check_output(shlex.split(mount_cmd))
        except subprocess.CalledProcessError:
            logging.error(f"Couldn't mount bucket {bucket_name}")
        if is_path_mounted(bucket_path):
            logging.info("success")
        else:
            logging.error(f"s3fs silently failed on path {bucket_path}. Are you sure this is correct?")


if __name__ == "__main__":
    argh.dispatch_commands([go])
