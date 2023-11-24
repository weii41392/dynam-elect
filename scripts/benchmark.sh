#!/bin/bash

python kvs_cluster.py --num_nodes 9 --benchmark &
sleep 5
python kvs_client.py --num_nodes 9 --num_requests 1000 --num_measurement 10 &
