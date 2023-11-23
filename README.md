# On Efficiency of Raft Algorithm under Uneven Network Conditions

## Install

- Python 3.10

- Install requirements
```
pip install -r requirements.txt
```

## Evaluation

### Key-Value Store

- Run cluster
```
python kvs_cluster.py --num_nodes NUM_NODES
```

- Run client
```
python kvs_client.py --num_nodes NUM_NODES
```

## Reference

- [Raft](https://raft.github.io/)
- [zhebrak/raftos](https://github.com/zhebrak/raftos)
