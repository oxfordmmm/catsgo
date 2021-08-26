## How to add a bucket

- add bucket to buckets.txt

buckets.txt format:

bucket1,
bucket2,
bucket3

No comma following final bucket

- run oracle_buckets.py to mount buckets

python3 oracle_buckets.py go --bucket-names "$(cat buckets.txt)"

- start dir_watcher systemd service for bucket:

systemctl --user restart dir_watcher@NEW_BUCKET_NAME
