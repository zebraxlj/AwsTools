
class LambdaCurrencyPoller:
    def __init__(self, interval_sec: int):
        self.interval_sec = interval_sec

    async def poll_once_async(self):
        pass
