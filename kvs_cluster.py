import argparse
import asyncio
import logging
import multiprocessing
import os

from aiohttp import web
import raftos
from raftos.log import logger

# Server ID: 0 ~ N - 1
# HTTP Port: 9000 + Server ID
# UDP Port: 8000 + Server ID
HTTP_PORT = 9000
UDP_PORT = 8000

def get_address(node_id):
    return '127.0.0.1:{}'.format(UDP_PORT + node_id)

def get_node_id(address):
    return int(address.rsplit(":", 1)[1]) - UDP_PORT


def setup_server(node_id):
    data = raftos.ReplicatedDict(name='data')
    routes = web.RouteTableDef()

    @routes.get('/get')
    async def get(request: web.Request):
        key = request.url.query.get("key")
        logger.info(f"/get?key={key}")
        if key is None:
            logger.info("Respond 400: key is required")
            return web.Response(status=400, text="key is required")

        leader = get_node_id(raftos.get_leader())
        if leader != node_id:
            logger.info(f"Respond 302: {leader}")
            return web.Response(status=302, text=str(leader))

        try:
            value = await data[key]
            logger.info(f"Respond 200: {value}")
            return web.Response(text=str(value))
        except KeyError:
            logger.info("Respond 404: key not found")
            return web.Response(status=404, text="key not found")

    @routes.put('/put')
    async def put(request: web.Request):
        key = request.url.query.get("key")
        value = request.url.query.get("value")
        logger.info(f"/put?key={key}&value={value}")
        if key is None or value is None:
            logger.info("Respond 400: key and value are required")
            return web.Response(status=400, text="key and value are required")

        leader = get_node_id(raftos.get_leader())
        if leader != node_id:
            logger.info(f"Respond 302: {leader}")
            return web.Response(status=302, text=str(leader))

        await data.update({key: value})
        logger.info("Respond 200: ok")
        return web.Response(text="ok")

    app = web.Application()
    app.add_routes(routes)
    return app.make_handler()

def main(node_id, node, cluster, log_base_dir, benchmark):
    log_dir = os.path.join(log_base_dir, f'node-{node_id}')
    os.makedirs(log_dir)
    logging.basicConfig(
        filename=os.path.join(log_dir, 'logging.log'),
        format=u'[%(asctime)s %(filename)s:%(lineno)d %(levelname)s]  %(message)s',
        level=logging.DEBUG if not benchmark else logging.ERROR
    )

    raftos.configure({
        'log_path': log_dir,
        'serializer': raftos.serializers.JSONSerializer
    })

    loop = asyncio.get_event_loop()
    loop.create_task(raftos.register(node, cluster=cluster))
    f = loop.create_server(setup_server(node_id), '127.0.0.1', HTTP_PORT + node_id)
    loop.run_until_complete(f)
    loop.run_forever()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--num_nodes', type=int, default=3)
    parser.add_argument('-l', '--log_dir', default='./logs')
    parser.add_argument('-b', '--benchmark', action='store_true')
    parser.add_argument('-r', '--remove', action='store_true')
    args = parser.parse_args()

    if args.remove and os.path.exists(args.log_dir):
        os.system(f"rm -rf {args.log_dir}")
    os.makedirs(args.log_dir)

    cluster = [get_address(node_id) for node_id in range(args.num_nodes)]
    processes = set()
    try:
        for i, node in enumerate(cluster):
            node_args = (i, node, cluster, args.log_dir, args.benchmark)
            process = multiprocessing.Process(target=main, args=node_args)
            process.start()
            processes.add(process)
        while processes:
            for process in tuple(processes):
                process.join()
                processes.remove(process)
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
