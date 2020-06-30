import asyncio
import os
from typing import Dict, Optional, Any, Union, Tuple
from ssl import SSLContext

from .base import ConnectionListener, Publisher
from .stats import AioStompStats
from .subscription import Subscription
from .log import logger
from .frame import Frame
from .protocol import StompProtocol
from .utils import encode


class AioStomp(Publisher):
    def __init__(
        self,
        host: str,
        port: int,
        ssl_context: Optional[SSLContext] = None,
        client_id: Optional[str] = None,
        reconnect_max_attempts: int = -1,
        reconnect_timeout: int = 1000,
        heartbeat_enable: bool = True,
        heartbeat: Tuple[int, int] = (1000, 1000),
        auto_decode: bool = True,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        self._heartbeat = {
            "enabled": heartbeat_enable,
            "heartbeat": heartbeat,
        }

        self._host = host
        self._port = port
        self._loop = loop or asyncio.get_event_loop()

        self._stats = None
        self.enable_stats = bool(os.environ.get("AIOSTOMP_ENABLE_STATS", False))
        if self.enable_stats:
            self._stats = AioStompStats()
            self._stats_handler = self._loop.create_task(self._stats.run())
        self._subscriptions: Dict[str, Subscription] = {}

        self._protocol = StompProtocol(
            self,
            host,
            port,
            self._loop,
            heartbeat=self._heartbeat,
            ssl_context=ssl_context,
            client_id=client_id,
            stats=self._stats,
            subscriptions=self._subscriptions,
            auto_decode=auto_decode,
        )
        self._last_subscribe_id = 0

        self._connected = False
        self._closed = False
        self._username: Optional[str] = None
        self._password: Optional[str] = None

        self._retry_interval = 0.5
        self._is_retrying = False

        self._reconnect_max_attempts = reconnect_max_attempts
        self._reconnect_timeout = reconnect_timeout / 1000.0
        self._reconnect_attempts = 0

    async def connect(
        self, username: Optional[str] = None, password: Optional[str] = None
    ) -> None:
        logger.debug("connect")
        self._username = username
        self._password = password

        await self._reconnect()

    def _resubscribe_queues(self) -> None:
        for subscription in self._subscriptions.values():
            self._protocol.subscribe(subscription)

    def _increment_retry_interval(self) -> None:
        self._reconnect_attempts += 1
        self._retry_interval = min(60.0, 1.5 * self._retry_interval)

    def _should_retry(self) -> bool:
        if self._reconnect_max_attempts == -1:
            return True

        if self._reconnect_attempts < self._reconnect_max_attempts:
            return True

        return False

    async def _reconnect(self) -> None:
        self._is_retrying = True
        while True:
            try:
                logger.info("Connecting to stomp server: %s:%s", self._host, self._port)

                await self._protocol.connect(
                    username=self._username, password=self._password
                )

                self._retry_interval = 0.5
                self._reconnect_attempts = 0
                self._is_retrying = False
                self._connected = True

                if self._stats:
                    self._stats.new_connection()

                self._resubscribe_queues()
                return

            except OSError:
                logger.info("Connecting to stomp server failed.")

                if self._should_retry():
                    logger.info("Retrying in %s seconds", self._retry_interval)
                    await asyncio.sleep(self._retry_interval)
                else:
                    logger.error("All connections attempts failed.")
                    break

                self._increment_retry_interval()

    def close(self) -> None:
        # Indicate requested closure to break the reconnect loop
        self._closed = True

        self._connected = False
        self._protocol.close()

        if self.enable_stats:
            self._stats_handler.cancel()

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self._connected = False

        # If close has been requested, do no reconnect!
        if self._closed:
            return

        if not self._is_retrying:
            logger.info("Connection lost, will retry.")
            self._is_retrying = True
            self._loop.call_soon(lambda: asyncio.ensure_future(self._reconnect(), loop=self._loop))
            # asyncio.ensure_future(self._reconnect(), loop=self._loop)

    def subscribe(
        self,
        destination: str,
        ack: str = "auto",
        extra_headers=None,
        auto_ack=True,
    ) -> Subscription:
        extra_headers = extra_headers or {}
        self._last_subscribe_id += 1

        subscription = Subscription(
            destination=destination,
            _id=self._last_subscribe_id,
            ack=ack,
            extra_headers=extra_headers,
            auto_ack=auto_ack,
        )

        self._subscriptions[str(self._last_subscribe_id)] = subscription

        if self._connected:
            self._protocol.subscribe(subscription)

        return subscription

    def unsubscribe(self, subscription: Subscription) -> None:
        subscription_id = str(subscription.id)

        if subscription_id in self._subscriptions.keys():
            self._protocol.unsubscribe(subscription)
            del self._subscriptions[subscription_id]

    def send(
        self,
        destination: str,
        body: Union[str, bytes] = "",
        headers: Optional[Dict[str, Any]] = None,
        send_content_length=True,
    ) -> None:
        headers = headers or {}
        headers["destination"] = destination

        body_b = encode(body)

        # ActiveMQ determines the type of a message by the
        # inclusion of the content-length header
        if send_content_length:
            headers["content-length"] = len(body_b)

        self._protocol.send(headers, body_b)

    @property
    def transport(self):
        return self._protocol.transport

    def _subscription_auto_ack(self, frame: Frame) -> bool:
        key = frame.headers.get("subscription", "")

        subscription = self._subscriptions.get(key)
        if not subscription:
            logger.warning("Subscription %s not found", key)
            return True

        if subscription.auto_ack:
            logger.warning("Auto ack/nack is enabled. Ignoring call.")
            return True

        return False

    def ack(self, frame: Frame) -> None:
        if self._subscription_auto_ack(frame):
            return

        self._protocol.ack(frame)

    def nack(self, frame: Frame) -> None:
        if self._subscription_auto_ack(frame):
            return

        self._protocol.nack(frame)

    def get(self, key: str) -> Optional[Subscription]:
        return self._subscriptions.get(key)

    def set_listener(self, name: str, listener: ConnectionListener) -> None:
        self._protocol.set_listener(name, listener)

    def remove_listener(self, name: str) -> None:
        self._protocol.remove_listener(name)

    def get_listener(self, name: str) -> ConnectionListener:
        return self._protocol.get_listener(name)
