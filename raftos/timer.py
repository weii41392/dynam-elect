import asyncio


#  schedule and execute periodic callbacks at a specified interval
class Timer:
    """Scheduling periodic callbacks"""
    def __init__(self, interval, callback):
        self.interval = interval  # the time interval between callbacks
        self.callback = callback  # the function to be called periodically
        self.loop = asyncio.get_event_loop()

        self.is_active = False  # indicate the timer is not running at the begining

    def start(self):
        self.is_active = True   #  indicate the timer is running.
        self.handler = self.loop.call_later(self.get_interval(), self._run)

    # If the timer is active, it executes the callback and 
    # schedules the next invocation.
    def _run(self):
        if self.is_active:
            self.callback()
            self.handler = self.loop.call_later(self.get_interval(), self._run)

    # Stops the timer by setting is_active to False and cancels 
    # the scheduled handler.
    def stop(self):
        self.is_active = False
        self.handler.cancel()

    # Stop and then restart the timer
    def reset(self):
        self.stop()
        self.start()

    def get_interval(self):
        return self.interval() if callable(self.interval) else self.interval
