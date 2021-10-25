#!/bin/bash

if [ -f "/home/ubuntu/catsgo/buckets.txt" ]
then
    cat /home/ubuntu/catsgo/buckets.txt | while read line
    do
        serv=dir_watcher@${line//,}.service
        echo "starting ${serv}"
	systemctl --user start $serv 
    done
fi