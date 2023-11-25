import time

from .log import logger


class TimeRecord:
    """Helper class used to measure RTT.
    Should not be used in evaluation.
    """

    def __init__(self):
        self.__record = {}

    def send(self, destination, type: str):
        if type.endswith("_response"):
            return
        self.__record[(destination, type)] = time.time()

    def receive(self, sender, type: str):
        if not type.endswith("_response") or \
            (sender, type[:-9]) not in self.__record:
            return
        rtt = time.time() - self.__record[(sender, type[:-9])]
        logger.debug(f"RTT to {sender}: {rtt}")


record = TimeRecord()
