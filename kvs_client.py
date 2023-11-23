import argparse
import logging
import random
import requests
import time

logging.basicConfig(format=u'[%(asctime)s %(filename)s:%(lineno)d %(levelname)s] %(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Server ID: 0 ~ N - 1
# HTTP Port: 9000 + Server ID
HTTP_PORT = 9000

def get(server, key):
    logging.info(f'{server}/get?key={key}')
    response = requests.get(f'{server}/get?key={key}')
    logging.info(f'Return {response.status_code}: {response.text}')
    return response.status_code, response.text

def put(server, key, value):
    logging.info(f'{server}/put?key={key}&value={value}')
    response = requests.put(f'{server}/put?key={key}&value={value}')
    logging.info(f'Return {response.status_code}: {response.text}')
    return response.status_code, response.text

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--num_nodes', type=int, default=3)
    args = parser.parse_args()

    assert args.num_nodes > 0

    servers = { i: f'http://127.0.0.1:{HTTP_PORT + i}' for i in range(args.num_nodes) }
    server_ids = list(servers.keys())

    server_id = random.choice(server_ids)
    while True:
        key = random.randint(0, 10)
        value = random.randint(0, 1000)

        status_code, text = put(servers[server_id], key, value)
        if status_code == 302:
            server_id = int(text)
            status_code, text = put(servers[server_id], key, value)
            if status_code != 200:
                logging.error(
                    f'Unexpected failure: put({servers[server_id]}, {key}, {value}), ' \
                    f'code={status_code}, text={text}')
                continue

        status_code, text = get(servers[server_id], key)
        if status_code == 302:
            server_id = int(text)
            status_code, text = put(servers[server_id], key, value)
            if status_code != 200:
                logging.error(
                    f'Unexpected failure: get({servers[server_id]}, {key}), ' \
                    f'code={status_code}, text={text}')
                continue

        if int(text) == value:
            logging.info('Put successfully')
        else:
            logging.info('Put failed')

        time.sleep(1)
