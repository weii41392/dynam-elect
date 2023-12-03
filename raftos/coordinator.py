"""Coordinator handle the states of nodes in cluster, i.e. leader,
candidate, and follower.

When coordinator wants to nominate the next leader, it invokes `to_candidate`
method of the target node and triggers the election process. The
`election_timer` of followers is still working, so the node nominated by the
coordinator isn't guaranteed to be the next leader.

## Heartbeat Interval

- The coordinator and the leader has a heartbeat interval. The coordinator
maintains the exponential moving average (EMA) of the round-trip time of
heartbeat for all previous and current leaders.

- The coordinator determines the current leader's next leadership duration
using EMA. The next duration is the current duration multiplied by some
constant between 0.5 and 2.

## Leader selection

- When the current leader is about to step down, the coordinator would
nominate the next candidate using this policy: with a probability of $p$ it
selects the node with the highest leadership duration, and with a
probability of $1 - p$ it selects the node randomly. This provides an
opportunity for nodes that initially experienced longer network delays but
possess better network characteristics to emerge as leaders.

- When the coordinator finds the round-trip time of the current leader
above a threshold (k * EMA, say k = 2), it aborts its leadership by
invoking `to_follower` method.

- When a leader encounters a network partition (handled by
`step_down_timer`), it notifies the coordinator.
"""

import asyncio
import math
import os
import random
import time

from .conf import config
from .log import logger
from .server import Node
from .state import State
from .storage import FileDict
from .timer import Timer


async def register(address, cluster=None):
    """Start Coordinator
    Args:
        address — 127.0.0.1:8000
        cluster — [127.0.0.1:8001, 127.0.0.1:8002, ...]
    """
    loop = asyncio.get_event_loop()
    host, port = address.rsplit(':', 1)
    node = CoordinatorNode(address=(host, int(port)), cluster=cluster, loop=loop)
    await node.start()

class CoordinatorNode(Node):
    def __init__(self, address, cluster, loop):
        self.host, self.port = address
        self.cluster = set()

        for address in cluster:
            host, port = address.rsplit(':', 1)
            port = int(port)
            if (host, port) != (self.host, self.port):
                self.update_cluster((host, port))

        self.loop = loop
        self.state = CoordinatorState(self)
        self.requests = asyncio.Queue()
        self.__class__.nodes.append(self)


class CoordinatorState(State):
    def __init__(self, server: CoordinatorNode):
        self.server = server
        self.id = self._get_id(server.host, server.port)
        self.__class__.loop = self.server.loop
        self.state = Coordinator(self)

        # TODO: add persistent log
        # self.log = CoordinatorFileStorage(self.id)


class Coordinator:
    def __init__(self, state: CoordinatorState):
        self.state = state
        # TODO: add persistent log
        # self.log = self.state.log

        self.id = self.state.id
        self.loop = self.state.loop

        self.initial_rotation = random.sample(
            self.state.cluster, len(self.state.cluster))
        self.duration = {
            node: config.initial_leadership_duration \
            for node in self.state.cluster
        }
        self.ema_rtt = {
            node: None for node in self.state.cluster
        }

        self.heartbeat_timer = Timer(config.heartbeat_interval, self.heartbeat)
        self.heartbeat_sent_at = None
        self.abort_leadership_timer = None

    @property
    def is_initial_rotation_over(self):
        return len(self.initial_rotation) == 0

    def nominate_candidate(self):
        """Nominate a node to become a candidate"""
        def select_nominee():
            if not self.is_initial_rotation_over:
                return self.initial_rotation[-1]
            if random.random() < config.probability_choosing_longest:
                return max(self.duration, key=self.duration.get)
            return random.choice(self.state.cluster)

        nominee = select_nominee()
        data = {
            'type': 'nominate_candidate',
            'duration': self.duration[nominee]
        }
        asyncio.ensure_future(self.state.send(data, nominee), loop=self.loop)
        logger.info(f"Send nominate_candidate RPC to {nominee}")
        logger.info(f"Duration, EMA: {', '.join(map(lambda k: f'{k}: ({self.duration[k]}, {self.ema_rtt[k]})', sorted(self.duration.keys())))}")

    def abort_leadership(self):
        """Early abort a leader"""
        leader_id = self.state.get_leader()
        if leader_id is None:
            self.stop()
            return
        data = { 'type': 'abort_leadership' }
        asyncio.ensure_future(self.state.send(data, leader_id), loop=self.loop)
        logger.info(f"Send abort_leadership RPC to {leader_id}")
        self.handle_end_leadership()

    def heartbeat(self):
        """Measure RTT between it and the leader
        If the leader doesn't respond for a long time, abort its leadership.
        """
        leader_id = self.state.get_leader()
        if leader_id is None:
            self.stop()
            return
        data = { 'type': 'heartbeat' }
        asyncio.ensure_future(self.state.send(data, leader_id), loop=self.loop)
        self.heartbeat_sent_at = time.time()
        self.heartbeat_timer.reset()

    def on_receive_heartbeat_response(self, data):
        leader_id = self.state.get_leader()
        sender_id = self.state.get_sender_id(data['sender'])
        if sender_id != leader_id or self.heartbeat_sent_at is None:
            return
        rtt = time.time() - self.heartbeat_sent_at
        self.heartbeat_sent_at = None

        # Update EMA
        self.ema_rtt[leader_id] = self.update_ema(self.ema_rtt[leader_id], rtt)
        logger.debug(f"Receive heartbeat response from {data['sender']}, rtt = {rtt}, ema = {self.ema_rtt[leader_id]}")

        # Early abort a leader when it performs poorly
        if self.is_initial_rotation_over:
            rtts = [v for v in self.ema_rtt.values() if v is not None]
            if not rtts:
                return
            avg_rtt = sum(rtts) / len(rtts)
            if avg_rtt * config.degradation_threshold < self.ema_rtt[leader_id]:
                logger.info("Leader is too slow. Aborting.")
                self.abort_leadership()

    def on_receive_start_leadership(self, data):
        logger.info(f"Receive StartLeadership RPC from {data['sender']}")
        self.stop()

        # Abort current leader
        leader_id = self.state.get_leader()
        if leader_id is not None:
            self.abort_leadership()

        # Accept new leader
        sender_id = self.state.get_sender_id(data['sender'])
        if not self.is_initial_rotation_over:
            self.initial_rotation.remove(sender_id)
        self.state.set_leader(sender_id)
        self.abort_leadership_timer = Timer(
            self.duration[sender_id],
            self.abort_leadership
        )
        self.abort_leadership_timer.start()
        self.heartbeat_timer.start()

    def on_receive_end_leadership(self, data):
        logger.info(f"Receive EndLeadership RPC from {data['sender']}")
        leader_id = self.state.get_leader()
        sender_id = self.state.get_sender_id(data['sender'])
        if sender_id == leader_id:
            self.handle_end_leadership()
        self.nominate_candidate()

    @staticmethod
    def update_ema(ema, new_value, momentum=config.ema_momentum):
        if ema is None:
            return new_value
        return (1 - momentum) * ema + momentum * new_value

    def update_duration(self, leader_id):
        if not self.is_initial_rotation_over or self.duration[leader_id] == \
            config.initial_leadership_duration:
            self.duration[leader_id] = config.post_rotation_leadership_duration

        if not self.is_initial_rotation_over:
            return

        # Calculate performance by taking reciprocal of ema rtt
        perfs = {k: 1 / (v + 1e-99) for k, v in self.ema_rtt.items() if v is not None}
        max_perf, min_perf = max(perfs.values()), min(perfs.values())

        # Normalize performance to [0.5, 2] to get multipliers
        multipliers = {
            k: (v - min_perf) / (max_perf - min_perf + 1e-99) * 1.5 + 0.5 \
            for k, v in perfs.items()
        }

        # Update duration
        for k, v in multipliers.items():
            self.duration[k] = max(
                config.lower_bound_leadership_duration,
                self.duration[k] * v
            )

    def handle_end_leadership(self):
        """Called when the current leader ends or we abort it"""
        leader_id = self.state.get_leader()
        if leader_id is None:
            return
        self.stop()
        self.state.set_leader(None)
        self.update_duration(leader_id)

    def start(self):
        self.nominate_candidate()

    def stop(self):
        try:
            self.heartbeat_timer.stop()
        except:
            pass
        try:
            self.abort_leadership_timer.stop()
        except:
            pass


# TODO: add persistent log
# class CoordinatorFileStorage(FileDict):
#     def __init__(self, node_id):
#         filename = os.path.join(config.log_path, '{}.storage'.format(node_id))
#         super().__init__(filename)
