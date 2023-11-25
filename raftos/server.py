import asyncio
import functools

from .conf import config
from .log import logger
from .network import UDPProtocol
from .state import State
from .time_record import record


async def register(*address_list, coordinator=None, cluster=None, loop=None):
    """Start Raft node (server)
    Args:
        address_list — 127.0.0.1:8000 [, 127.0.0.1:8001 ...]
        coordinator  - 127.0.0.1.8001
        cluster — [127.0.0.1:8002, 127.0.0.1:8003, ...]
    """

    loop = loop or asyncio.get_event_loop()
    for address in address_list:
        host, port = address.rsplit(':', 1)
        node = Node(address=(host, int(port)), loop=loop)
        if coordinator:
            host, port = coordinator.rsplit(':', 1)
            port = int(port)
            node.set_coordinator((host, port))

        for address in cluster:
            host, port = address.rsplit(':', 1)
            port = int(port)

            if (host, port) != (node.host, node.port):
                node.update_cluster((host, port))
        await node.start()


def stop():
    for node in Node.nodes:
        node.stop()


class Node:
    """Raft Node (Server)"""

    nodes = []

    def __init__(self, address, loop):
        self.host, self.port = address
        self.cluster = set()
        self.coordinator = None

        self.loop = loop
        self.state = State(self)
        self.requests = asyncio.Queue()
        self.__class__.nodes.append(self)

    async def start(self):
        protocol = UDPProtocol(
            queue=self.requests,
            request_handler=self.request_handler,
            loop=self.loop,
            delay=config.simulated_delay(self.port)
        )
        address = self.host, self.port
        self.transport, _ = await asyncio.Task(
            self.loop.create_datagram_endpoint(protocol, local_addr=address),
            loop=self.loop
        )
        logger.info(f"Node started")
        self.state.start()

    def stop(self):
        self.state.stop()
        self.transport.close()

    def update_cluster(self, address_list):
        self.cluster.update({address_list})

    @property
    def cluster_count(self):
        return len(self.cluster)

    def set_coordinator(self, address):
        self.coordinator = address

    def request_handler(self, data):
        self.state.request_handler(data)

    async def send(self, data, destination):
        """Sends data to destination Node
        Args:
            data — serializable object
            destination — <str> '127.0.0.1:8000' or <tuple> (127.0.0.1, 8000)
        """
        if isinstance(destination, str):
            host, port = destination.split(':')
            destination = host, int(port)

        # logger.debug(f"Send {data['type']} RPC to {destination}")
        await self.requests.put({
            'data': data,
            'destination': destination
        })
        # record.send(destination, data['type'])

    def broadcast(self, data):
        """Sends data to all Nodes in cluster (cluster list does not contain self Node)"""
        for destination in self.cluster:
            asyncio.ensure_future(self.send(data, destination), loop=self.loop)
