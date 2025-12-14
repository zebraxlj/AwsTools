
class PollerBase:
    def __init__(self, interval_sec: int, args, kwargs):
        self.args = args
        self.kwargs = kwargs

    def poll(self):
        pass
        # self.waiter.wait(client=self.client, *self.args, **self.kwargs)

