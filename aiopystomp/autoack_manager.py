from .base import StompBaseProtocol
from .subscription import Subscription
from .frame import Frame


class AutoAckManager:
    allow_ack = {"client", "client-individual"}

    def __init__(self, protocol: "StompBaseProtocol") -> None:
        self.protocol = protocol

    def auto_ack(self, frame: Frame, subscription: Subscription):
        if not frame:
            return

        if not subscription.auto_ack:
            return

        if subscription.ack in self.allow_ack:
            self.protocol.ack(frame)
