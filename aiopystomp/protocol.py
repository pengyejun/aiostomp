# -*- coding:utf-8 -*-
import asyncio
import functools
import uuid
from ssl import SSLContext
from typing import Dict, Union, Any, Optional, cast

from .base import StompBaseProtocol, ConnectionListener, Publisher
from .constans import MESSAGE, CONNECTED, ERROR, CMD_SEND, CMD_ACK, CMD_NACK, HEARTBEAT, CMD_CONNECT, HDR_MESSAGE_ID, \
    HDR_SUBSCRIPTION, CMD_SUBSCRIBE, CMD_UNSUBSCRIBE, HDR_DESTINATION, HDR_ID, HDR_ACK, HDR_HEARTBEAT, \
    HDR_ACCEPT_VERSION, HDR_PASSCODE, HDR_LOGIN
from .autoack_manager import AutoAckManager
from .errors import StompDisconnectedError
from .heartbeat import StompHeartBeater
from .frame import Frame
from .stats import AioStompStats
from .subscription import Subscription
from .transport import AmqProtocol
from .log import logger

HandlerMap = {CONNECTED: "on_connected", HEARTBEAT: "on_heartbeat", MESSAGE: "on_message", ERROR: "on_error"}


class StompProtocol(StompBaseProtocol, Publisher):
    def __init__(
        self,
        handler,
        host: str,
        port: int,
        loop: asyncio.AbstractEventLoop,
        heartbeat: Optional[Dict[str, Any]] = None,
        ssl_context: Optional[SSLContext] = None,
        client_id: Optional[str] = None,
        stats: Optional[AioStompStats] = None,
        subscriptions: Dict[str, Subscription] = None,
        auto_decode: bool = True,
    ):

        self.host = host
        self.port = port
        self.ssl_context = ssl_context
        self.client_id = client_id
        self.auto_decode = auto_decode
        self._stats = stats
        self.subscriptions = subscriptions
        if loop is None:
            loop = asyncio.get_event_loop()
        self.listener: Dict[str, ConnectionListener] = {}
        self._loop = loop
        self._heartbeat = heartbeat or {}
        self._handler = handler
        self._protocol: Optional[BaseProtocol] = None

    async def connect(self, username: Optional[str] = None, password: Optional[str] = None) -> None:
        self._factory = functools.partial(
            BaseProtocol,
            self,
            username=username,
            password=password,
            client_id=self.client_id,
            loop=self._loop,
            heartbeat=self._heartbeat,
            stats=self._stats,
            auto_decode=self.auto_decode,
        )

        trans, proto = await self._loop.create_connection(
            self._factory, host=self.host, port=self.port, ssl=self.ssl_context
        )

        self._transport = trans
        self._protocol = cast(BaseProtocol, proto)

    def close(self) -> None:
        if self._protocol:
            self._protocol.close()

    def subscribe(self, subscription: Subscription) -> None:
        if self._protocol is None:
            raise RuntimeError("Not connected")
        headers = {
            HDR_ACK: subscription.ack,
            HDR_DESTINATION: subscription.destination,
            HDR_ID: subscription.id,
        }
        headers.update(subscription.extra_headers)

        self._protocol.send_frame(CMD_SUBSCRIBE, headers)

    def unsubscribe(self, subscription: Subscription) -> None:
        if self._protocol is None:
            raise RuntimeError("Not connected")
        headers = {
            HDR_ID: subscription.id,
            HDR_DESTINATION: subscription.destination
        }
        self._protocol.send_frame(CMD_UNSUBSCRIBE, headers)

    def send(self, headers: Dict[str, Any], body: Union[bytes, str]) -> None:
        if self._protocol is None:
            raise RuntimeError("Not connected")
        self._protocol.send_frame(CMD_SEND, headers, body)

    @property
    def transport(self):
        return self._protocol.transport

    def ack(self, frame: Frame) -> None:
        if self._protocol is None:
            raise RuntimeError("Not connected")
        headers = {
            HDR_SUBSCRIPTION: frame.headers[HDR_SUBSCRIPTION],
            HDR_MESSAGE_ID: frame.headers[HDR_MESSAGE_ID],
        }
        self._protocol.send_frame(CMD_ACK, headers)

    def nack(self, frame: Frame) -> None:
        if self._protocol is None:
            raise RuntimeError("Not connected")
        headers = {
            HDR_SUBSCRIPTION: frame.headers[HDR_SUBSCRIPTION],
            HDR_MESSAGE_ID: frame.headers[HDR_MESSAGE_ID],
        }
        self._protocol.send_frame(CMD_NACK, headers)

    def feed_data(self, frame: Frame):
        for _, listener in self.listener.items():
            method = HandlerMap.get(frame.command)
            if method:
                func = getattr(listener, method, None)
                if asyncio.iscoroutinefunction(func):
                    self._loop.create_task(func(frame))
                else:
                    func(frame)

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self._handler.connection_lost(exc)

    def set_listener(self, name: str, listener: ConnectionListener) -> None:
        self.listener[name] = listener

    def remove_listener(self, name: str) -> None:
        del self.listener[name]

    def get_listener(self, name: str) -> ConnectionListener:
        return self.listener[name]


class BaseProtocol(asyncio.Protocol):
    def __init__(self,
                 frame_handler: StompProtocol,
                 loop: asyncio.AbstractEventLoop,
                 heartbeat: Optional[Dict[str, Any]] = None,
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 client_id: Optional[str] = None,
                 auto_decode: bool = True,
                 stats: Optional[AioStompStats] = None):

        self.handlers_map = {
            MESSAGE: self._handle_message,
            CONNECTED: self._handle_connect,
            ERROR: self._handle_error,
        }

        self.heartbeat = heartbeat or {}
        self.heartbeater: Optional[StompHeartBeater] = None
        self._loop = loop
        self._frame_handler = frame_handler
        self._stats = stats
        self._transport: Optional[asyncio.Transport] = None
        self._protocol = AmqProtocol(auto_decode)
        self._connect_headers: Dict[str, str] = dict()
        self.auto_ack_manager = AutoAckManager(frame_handler)
        self._connect_headers[HDR_ACCEPT_VERSION] = "1.1"

        if client_id is not None:
            unique_id = uuid.uuid4()
            self._connect_headers["client-id"] = f"{client_id}-{unique_id}"

        if self.heartbeat.get("enabled"):
            self._connect_headers[HDR_HEARTBEAT] = f"{self.heartbeat['heartbeat'][0]},{self.heartbeat['heartbeat'][1]}"

        if username is not None:
            self._connect_headers[HDR_LOGIN] = username

        if password is not None:
            self._connect_headers[HDR_PASSCODE] = password

    def close(self) -> None:
        # Close the transport only if already connection is made
        if self._transport:
            # Close the transport to stomp receiving any more data
            self._transport.close()

        if self.heartbeater:
            self.heartbeater.shutdown()
            self.heartbeater = None

    def connect(self) -> None:
        self.send_frame(CMD_CONNECT, self._connect_headers)

    def send_frame(self, command: str, headers: Optional[Dict[str, Any]] = None,
                   body: Union[str, bytes] = b"",) -> None:
        if headers is None:
            headers = {}
        buf = self._protocol.build_frame(command, headers, body)

        if not self._transport:
            raise StompDisconnectedError()

        if self._stats:
            self._stats.increment("sent_msg")
        self._transport.write(buf)

    def connection_made(self, transport: asyncio.Transport) -> None:
        logger.info("Connected")
        self._transport = transport
        self.connect()

    @property
    def transport(self):
        return self._transport

    def connection_lost(self, exc: Optional[Exception]) -> None:
        logger.debug("connection lost")

        self._transport = None

        if self.heartbeater:
            self.heartbeater.shutdown()
            self.heartbeater = None

        self._frame_handler.connection_lost(exc)

    async def _handle_connect(self, frame: Frame) -> None:
        if self._transport is None:
            return
        heartbeat = frame.headers.get(HDR_HEARTBEAT)
        logger.debug("Expecting heartbeats: %s", heartbeat)
        if heartbeat and self.heartbeat.get("enabled"):
            sx, sy = (int(x) for x in heartbeat.split(","))
            if sy:
                interval = max(self.heartbeat.get("cx", 0), sy)
                logger.debug("Sending heartbeats every %sms", interval)
                self.heartbeater = StompHeartBeater(
                    self._transport, interval=interval, loop=self._loop
                )
                await self.heartbeater.start()

    async def _handle_message(self, frame: Frame) -> None:
        key = frame.headers.get(HDR_SUBSCRIPTION, "")

        subscription = self._frame_handler.subscriptions.get(key)
        if not subscription:
            logger.warning("Subscribe %s not found", key)
            return

        if self._stats:
            self._stats.increment("rec_msg")
        self.auto_ack_manager.auto_ack(frame, subscription)

    async def _handle_error(self, frame: Frame) -> None:
        message = frame.headers.get("message")

        logger.error("Received error: %s" % message)
        logger.debug("Error details: %s" % frame.body)

    async def _handle_exception(self, frame: Frame) -> None:
        logger.warning("Unhandled frame: %s", frame.command)

    def data_received(self, data: Optional[bytes]) -> None:
        if not data:
            return
        self._protocol.process_data(data)
        for frame in self._protocol.pop_frames():
            if frame.command == CONNECTED:
                print("CONNECTED")
                handle = self.handlers_map.get(frame.command, self._handle_exception)
                asyncio.ensure_future(handle(frame))

            self._frame_handler.feed_data(frame)

    def eof_received(self) -> None:
        self.connection_lost(Exception("Got EOF from server"))
