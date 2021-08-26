import sys
import os

import argh


def main():
    buckets = [line.strip() for line in open("buckets.txt").read().split(",")]

    for bucket in buckets:
        cmd = f"SYSTEMD_PAGER= systemctl status --user dir_watcher@{bucket}"
        os.system(cmd)
        print("")


if __name__ == "__main__":
    argh.dispatch_command(main)
