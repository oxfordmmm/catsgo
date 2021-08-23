import sys
import os

import argh


def main(no_prompt=False):
    buckets = [line.strip() for line in open("buckets.txt").read().split(",")]

    if not no_prompt:
        print("This will (re)start all dir_watchers.")
        print("To continue type Y and press enter")
        if input() != "Y":
            print("Aborted")
            sys.exit(1)

    for bucket in buckets:
        cmd = f"systemctl restart --user dir_watcher@{bucket}"
        print(cmd)
        os.system(cmd)


if __name__ == "__main__":
    argh.dispatch_command(main)
