from typing import Dict


class Subscription:
    def __init__(
        self,
        destination: str,
        id: int,
        ack: str,
        extra_headers: Dict[str, str],
        auto_ack: bool = True,
    ):
        self.destination = destination
        self.id = id
        self.ack = ack
        self.extra_headers = extra_headers
        self.auto_ack: bool = auto_ack
