import argparse
import asyncio
import logging
import multiprocessing
import os

from aiohttp import web
import raftos

logger = logging.getLogger('raftos')

# Coordinator: 0, Servers: 1 ~ N
# HTTP Port: 9000 + Server ID
# UDP Port: 8000 + Server ID
HTTP_PORT = 9000
UDP_PORT = 8000

def from_udp_to_http(address):
    host, port = address.split(':')
    return f"{host}:{int(port) + 1000}"

def get_udp_address(node_id):
    return f'127.0.0.1:{UDP_PORT + node_id}'

def leader_required(node_id):
    address = get_udp_address(node_id)
    def decorator(func):
        def wrapped(self, *args, **kwargs):
            leader = raftos.get_leader()
            if leader is None:
                logger.debug("Respond 503: no leader")
                return web.Response(status=503, text="no leader")
            if leader != address:
                leader = from_udp_to_http(leader)
                logger.debug(f"Respond 302: {leader}")
                return web.Response(status=302, text=leader)
            return func(self, *args, **kwargs)
        return wrapped
    return decorator

def setup_server(node_id):
    data = raftos.ReplicatedDict(name='data')
    routes = web.RouteTableDef()

    @routes.get('/get')
    @leader_required(node_id)
    async def get(request: web.Request):
        key = request.url.query.get("key")
        logger.debug(f"/get?key={key}")
        if key is None:
            logger.debug("Respond 400: key is required")
            return web.Response(status=400, text="key is required")
        try:
            value = await data[key]
            logger.debug(f"Respond 200: {value}")
            return web.Response(text=str(value))
        except KeyError:
            logger.debug("Respond 404: key not found")
            return web.Response(status=404, text="key not found")

    @routes.put('/put')
    @leader_required(node_id)
    async def put(request: web.Request):
        key = request.url.query.get("key")
        value = request.url.query.get("value")
        logger.debug(f"/put?key={key}&value={value}")
        if key is None or value is None:
            logger.debug("Respond 400: key and value are required")
            return web.Response(status=400, text="key and value are required")
        await data.update({key: value})
        logger.debug("Respond 200: ok")
        return web.Response(text="ok")

    @routes.post('/compact_log')
    @leader_required(node_id)
    async def compact_log(request: web.Request):
        logger.debug("/compact_log")
        await raftos.compact_log()
        logger.debug("Respond 200: ok")
        return web.Response(text="ok")

    app = web.Application()
    app.add_routes(routes)
    return app.make_handler()

def run_coordinator_node(coordinator, cluster, log_base_dir, benchmark):
    log_dir = os.path.join(log_base_dir, 'coordinator')
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
    loop.create_task(raftos.register_coordinator(coordinator, cluster))
    loop.run_forever()

def run_cluster_node(node_id, node, coordinator, cluster, log_base_dir, benchmark):
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
    loop.create_task(raftos.register(node, coordinator=coordinator, cluster=cluster))
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

    cluster = [get_udp_address(node_id) for node_id in range(args.num_nodes + 1)]
    coordinator, cluster = cluster[0], cluster[1:]
    processes = set()
    try:
        # Cluster nodes
        for i, node in enumerate(cluster):
            node_args = (i + 1, node, coordinator, cluster, args.log_dir, args.benchmark)
            process = multiprocessing.Process(target=run_cluster_node, args=node_args)
            process.start()
            processes.add(process)

        # Coordinator node
        node_args = (coordinator, cluster, args.log_dir, args.benchmark)
        process = multiprocessing.Process(target=run_coordinator_node, args=node_args)
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
