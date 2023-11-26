#!/bin/bash

python kvs_cluster.py --num_nodes 9 --benchmark &
sleep 30
python kvs_client.py --num_nodes 9 --num_requests 100 --num_measurement 10 &
