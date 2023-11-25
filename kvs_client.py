import argparse
import logging
import os
import random
import requests
import time

logger = logging.getLogger(__name__)
random.seed(42)

# Server ID: 1 ~ N
# HTTP Port: 9000 + Server ID
HTTP_PORT = 9000
server_address = None

def retry_and_redirect(func):
    def wrapped(*args, **kwargs):
        global server_address
        while True:
            status_code, text = func(*args, **kwargs)
            if status_code == 200:
                return status_code, text
            if status_code == 302:
                args = list(args)
                args[0] = text
                server_address = text
    return wrapped

@retry_and_redirect
def get(server, key):
    logger.debug(f'http://{server}/get?key={key}')
    try:
        response = requests.get(f'http://{server}/get?key={key}', timeout=1)
    except requests.exceptions.Timeout:
        return 408, None
    logger.debug(f'Return {response.status_code}: {response.text}')
    return response.status_code, response.text

@retry_and_redirect
def put(server, key, value):
    logger.debug(f'http://{server}/put?key={key}&value={value}')
    try:
        response = requests.put(f'http://{server}/put?key={key}&value={value}', timeout=1)
    except requests.exceptions.Timeout:
        return 408, None
    logger.debug(f'Return {response.status_code}: {response.text}')
    return response.status_code, response.text

@retry_and_redirect
def compact_log(server):
    logger.debug(f'http://{server}/compact_log')
    try:
        response = requests.post(f'http://{server}/compact_log', timeout=1)
    except requests.exceptions.Timeout:
        return 408, None
    logger.debug(f'Return {response.status_code}: {response.text}')
    return response.status_code, response.text

def measure_once(servers, num_requests):
    global server_address
    server_address = random.choice(list(servers.values()))
    keys = [random.randint(1, 10) for _ in range(num_requests)]
    values = list(range(num_requests))
    random.shuffle(keys)
    random.shuffle(values)
    start_time = time.perf_counter()
    for key, value in zip(keys, values):
        status_code, text = put(server_address, key, value)
        if status_code != 200:
            logger.error(
                f'Unexpected failure: put({server_address}, {key}, {value}), ' \
                f'code={status_code}, text={text}')
            continue

        status_code, text = get(server_address, key)
        if status_code != 200:
            logger.error(
                f'Unexpected failure: get({server_address}, {key}), ' \
                f'code={status_code}, text={text}')
            continue

        if int(text) == value:
            logger.debug('Put successfully')
        else:
            logger.error('Put failed')
    end_time = time.perf_counter()

    # Compact log
    status_code, text = compact_log(server_address)
    if status_code != 200:
        logger.error(
            f'Unexpected failure: get({server_address}, {key}), ' \
            f'code={status_code}, text={text}')

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
    log_path = os.path.join(args.log_dir, 'client', 'logging.log')
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
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

    servers = { i: f'127.0.0.1:{HTTP_PORT + i}' for i in range(1, args.num_nodes + 1) }
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
