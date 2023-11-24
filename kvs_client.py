import argparse
import logging
import os
import random
import requests
import time

logger = logging.getLogger(__name__)
random.seed(42)

# Server ID: 0 ~ N - 1
# HTTP Port: 9000 + Server ID
HTTP_PORT = 9000

def get(server, key):
    logger.debug(f'{server}/get?key={key}')
    response = requests.get(f'{server}/get?key={key}')
    logger.debug(f'Return {response.status_code}: {response.text}')
    return response.status_code, response.text

def put(server, key, value):
    logger.debug(f'{server}/put?key={key}&value={value}')
    response = requests.put(f'{server}/put?key={key}&value={value}')
    logger.debug(f'Return {response.status_code}: {response.text}')
    return response.status_code, response.text

def measure_once(servers, num_requests):
    server_id = random.choice(list(servers.keys()))
    keys = [random.randint(1, 10) for _ in range(num_requests)]
    values = list(range(num_requests))
    random.shuffle(keys)
    random.shuffle(values)
    start_time = time.perf_counter()
    for key, value in zip(keys, values):
        status_code, text = put(servers[server_id], key, value)
        if status_code == 302:
            server_id = int(text)
            status_code, text = put(servers[server_id], key, value)
        if status_code != 200:
            logger.error(
                f'Unexpected failure: put({servers[server_id]}, {key}, {value}), ' \
                f'code={status_code}, text={text}')
            continue

        status_code, text = get(servers[server_id], key)
        if status_code == 302:
            server_id = int(text)
            status_code, text = get(servers[server_id], key)
        if status_code != 200:
            logger.error(
                f'Unexpected failure: get({servers[server_id]}, {key}), ' \
                f'code={status_code}, text={text}')
            continue

        if int(text) == value:
            logger.debug('Put successfully')
        else:
            logger.error('Put failed')
    end_time = time.perf_counter()
    return end_time - start_time

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--num_nodes', type=int, default=3)
    parser.add_argument('-r', '--num_requests', type=int, default=100)
    parser.add_argument('-m', '--num_measurements', type=int, default=1)
    parser.add_argument('-l', '--log_dir', default='./logs')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    assert args.num_nodes > 0
    assert args.num_requests > 0
    assert args.num_measurements > 0
    os.makedirs(args.log_dir, exist_ok=True)
    log_path = os.path.join(args.log_dir, 'client-logging.log')
    assert not os.path.exists(log_path)

    logging.basicConfig(
        filename=log_path,
        format=u'[%(asctime)s %(filename)s:%(lineno)d %(levelname)s] %(message)s',
        level=logging.DEBUG if args.verbose else logging.INFO
    )

    logging.info(f"Number of nodes: {args.num_nodes}")
    logging.info(f"Number of requests: {args.num_requests}")
    logging.info(f"Number of measurements: {args.num_measurements}")
    logging.info("Start measurement")

    servers = { i: f'http://127.0.0.1:{HTTP_PORT + i}' for i in range(args.num_nodes) }
    if args.num_measurements == 1:
        measure_once(servers, args.num_requests)
    tps_list = []
    for i in range(args.num_measurements):
        m = measure_once(servers, args.num_requests)
        tps = args.num_requests / (m + 1e-99)
        logging.info(f"Measurement {i + 1}: {tps:.3f}")
        tps_list.append(tps)
    average_tps = sum(tps_list) / len(tps_list)
    logging.info(f"Average TPS (transaction per second): {average_tps:.3f}")
