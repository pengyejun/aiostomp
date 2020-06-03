from typing import Any, Optional

from .base import StompBaseProtocol
from .frame import Frame


class AutoAckContextManager:
    def __init__(
        self, protocol: "StompBaseProtocol", ack_mode: str = "auto", enabled: bool = True
    ) -> None:
        self.protocol = protocol
        self.enabled = enabled
        self.ack_mode = ack_mode
        self.result = None
        self.frame: Optional[Frame] = None

    def __enter__(self) -> "AutoAckContextManager":
        return self

    def __exit__(self, exc_type: type, exc_value: Exception, exc_traceback: Any) -> None:
        if not self.enabled:
            return

        if not self.frame:
            return

        if self.ack_mode in ["client", "client-individual"]:
            if self.result:
                self.protocol.ack(self.frame)
            else:
                self.protocol.nack(self.frame)
