from .conf import configure, config
from .coordinator import register as register_coordinator
from .replicator import Replicated, ReplicatedDict, ReplicatedList
from .server import register, stop
from .state import State


__all__ = [
    'Replicated',
    'ReplicatedDict',
    'ReplicatedList',

    'config',
    'configure',
    'register',
    'register_coordinator',
    'stop',

    'get_leader',
    'wait_until_leader',
    'compact_log'
]


get_leader = State.get_leader
wait_until_leader = State.wait_until_leader
compact_log = State.compact_log
