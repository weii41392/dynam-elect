from .cryptors import default_cryptor
from .serializers import MessagePackSerializer


class Configuration:
    def __init__(self):
        self.configure(self.default_settings())

    @staticmethod
    def default_settings():
        return {
            'log_path': '/var/log/raftos/',
            'serializer': MessagePackSerializer,

            'heartbeat_interval': 0.3,

            # Leader will step down if it doesn't have a majority of follower's responses
            # for this amount heartbeats
            'step_down_missed_heartbeats': 5,

            # Randomized election timeout
            # [step_down_missed_heartbeats, M * step_down_missed_heartbeats]
            'election_interval_spread': 3,

            # For UDP messages encryption
            'secret_key': b'raftos sample secret key',
            'salt': b'raftos sample salt',
            'cryptor': default_cryptor,

            # Election callbacks
            'on_leader': lambda: None,
            'on_follower': lambda: None,

            'simulated_delay': lambda port: (port - 8000) * 1e-4,
            'initial_leadership_duration': 2,
            'post_rotation_leadership_duration': 600,
            'probability_choosing_longest': 0.9,
            'degradation_threshold': 3,
            'ema_momentum': 2e-2,
        }

    def configure(self, kwargs):
        for param, value in kwargs.items():
            setattr(self, param.lower(), value)

        self.step_down_interval = self.heartbeat_interval * self.step_down_missed_heartbeats
        self.election_interval = (
            self.step_down_interval,
            self.step_down_interval * self.election_interval_spread
        )

        self.wait_before_respond_vote_request = self.step_down_interval * 0.2
        self.lower_bound_leadership_duration = self.initial_leadership_duration

        if isinstance(self.cryptor, type):
            self.cryptor = self.cryptor(self)


config = Configuration()
configure = config.configure
