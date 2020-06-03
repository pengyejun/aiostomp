# -*- coding:utf-8 -*-
import asyncio
import functools
import logging
import itertools
import uuid
from ssl import SSLContext
from collections import deque, OrderedDict
from typing import List, Dict, Union, Any, Optional, Deque, cast, OrderedDict as _OrderedDict

from .base import StompBaseProtocol, ConnectionListener, Publisher
from .constans import MESSAGE, CONNECTED, ERROR, CMD_SEND, CMD_ACK, CMD_NACK, HEARTBEAT, CMD_CONNECT, HDR_MESSAGE_ID, \
    HDR_SUBSCRIPTION, CMD_SUBSCRIBE, CMD_UNSUBSCRIBE, HDR_DESTINATION, HDR_ID, HDR_ACK, HDR_HEARTBEAT, \
    HDR_ACCEPT_VERSION
from .ctx_manager import AutoAckContextManager
from .errors import StompDisconnectedError, StompError
from .heartbeat import StompHeartBeater
from .frame import Frame
from .stats import AioStompStats
from .subscription import Subscription

logger = logging.getLogger("aio_stomp.protocol")

HandlerMap = {CONNECTED: "on_connected", HEARTBEAT: "on_heartbeat", MESSAGE: "on_message", ERROR: "on_error"}


class Amq:
    V1_0 = "1.0"
    V1_1 = "1.1"
    V1_2 = "1.2"


class AmqProtocol:

    HEART_BEAT = b"\n"
    EOF = b"\x00"
    CRLFCRLR = [b"\r", b"\n", b"\r", b"\n"]
    HEADER_MAP = {"\n": "\\n", ":": "\\c", "\\": "\\\\", "\r": "\\r"}
    MAX_DATA_LENGTH = 1024 * 1024 * 100
    MAX_COMMAND_LENGTH = 1024

    def __init__(self) -> None:
        self._pending_parts: List[bytes] = []
        self._frames_ready: List[Frame] = []

        self.processed_headers = False
        self.awaiting_command = True
        self.read_length = 0
        self.content_length = -1
        self.previous_byte: Optional[bytes] = None

        self.action: Optional[str] = None
        self.headers: Dict[str, str] = {}
        self.current_command: Deque[int] = deque()

        self._version = Amq.V1_1

    def _decode(self, byte_data: Union[str, bytes, bytearray]) -> str:
        try:
            if isinstance(byte_data, (bytes, bytearray)):
                return byte_data.decode("utf-8")
            if isinstance(byte_data, str):
                return byte_data
            else:
                raise TypeError("Must be bytes or string")

        except UnicodeDecodeError:
            logging.error("string was: %s", byte_data)
            raise

    def _decode_header(self, header: bytes) -> str:
        decoded = []

        stream: Deque[int] = deque(header)
        has_data = True

        while has_data:
            if not stream:
                break

            _b = bytes([stream.popleft()])
            if _b == b"\\":
                if len(stream) == 0:
                    decoded.append(_b)
                else:
                    _next = bytes([stream.popleft()])
                    if _next == b"n":
                        decoded.append(b"\n")
                    elif _next == b"c":
                        decoded.append(b":")
                    elif _next == b"\\":
                        decoded.append(b"\\")
                    elif _next == b"r":
                        decoded.append(b"\r")
                    else:
                        stream.appendleft(_next[0])
                        decoded.append(_b)
            else:
                decoded.append(_b)

        return self._decode(b"".join(decoded))

    def _encode(self, value: Union[str, bytes]) -> bytes:
        if isinstance(value, str):
            return value.encode("utf-8")

        return value

    def _encode_header(self, header_value: Any) -> str:
        value = f"{header_value}"
        if self._version == Amq.V1_0:
            return value
        return "".join(self.HEADER_MAP.get(c, c) for c in value)

    def reset(self) -> None:
        self._frames_ready = []

    def process_data(self, data: bytes) -> None:
        read_size = len(data)
        data: Deque[int] = deque(data)
        i = 0

        while i < read_size:
            i += 1
            b = bytes([data.popleft()])

            if not self.processed_headers and self.previous_byte == self.EOF and b == self.EOF:
                continue

            if not self.processed_headers:
                if self.awaiting_command and b == b"\n":
                    self._frames_ready.append(
                        Frame(HEARTBEAT, headers={}, body=None))
                    continue
                else:
                    self.awaiting_command = False

                self.current_command.append(b[0])
                if b == b"\n" and (
                    self.previous_byte == b"\n" or ends_with_crlf(
                        self.current_command)):

                    try:
                        self.action = self._parse_action(self.current_command)
                        self.headers = self._parse_headers(
                            self.current_command)
                        logger.debug("Parsed action %s", self.action)

                        if (
                            self.action in (CMD_SEND, MESSAGE, ERROR)
                            and "content-length" in self.headers
                        ):
                            self.content_length = int(
                                self.headers["content-length"])
                        else:
                            self.content_length = -1
                    except Exception:
                        self.current_command.clear()
                        return

                    self.processed_headers = True
                    self.current_command.clear()
            else:
                if self.content_length == -1:
                    if b == self.EOF:
                        self.process_command()
                    else:
                        self.current_command.append(b[0])

                        if len(self.current_command) > self.MAX_DATA_LENGTH:
                            # error
                            return
                else:
                    if self.read_length == self.content_length:
                        self.process_command()
                        self.read_length = 0
                    else:
                        self.read_length += 1
                        self.current_command.append(b[0])

            self.previous_byte = b

    def process_command(self) -> None:
        body: Optional[bytes] = bytes(self.current_command)
        if body == b"":
            body = None
        frame = Frame(self.action or "", self.headers, body)
        self._frames_ready.append(frame)

        self.processed_headers = False
        self.awaiting_command = True
        self.content_length = -1
        self.current_command.clear()

    def _read_line(self, _input: Deque[int]) -> bytes:
        result = []
        line_end = False
        while not line_end:
            if not _input:
                break
            b = _input.popleft()
            if b == b"\n"[0]:
                break
            result.append(b)

        return bytes(result)

    def _parse_action(self, data: Deque[int]) -> str:
        action = self._read_line(data)
        return self._decode(action)

    def _parse_headers(self, data: Deque[int]) -> Dict[str, str]:
        headers = {}
        while True:
            line = self._read_line(data)
            if len(line) > 1:
                name, value = line.split(b":", 1)
                headers[self._decode(name)] = self._decode_header(value)
            else:
                break
        return headers

    def build_frame(self, command: str, headers: Dict[str, Any], body: Union[bytes, str] = "") -> bytes:
        lines: List[Union[str, bytes]] = [command, "\n"]

        for key, value in headers.items():
            lines.append(f"{key}:{self._encode_header(value)}\n")

        lines.append("\n")
        lines.append(body)
        lines.append(self.EOF)

        return b"".join(self._encode(line) for line in lines)

    def pop_frames(self) -> List[Frame]:
        frames = self._frames_ready
        self._frames_ready = []

        return frames


def ends_with_crlf(data: Deque[int]) -> bool:
    size = len(data)
    ending = list(itertools.islice(data, size - 4, size))

    return ending == AmqProtocol.CRLFCRLR


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
        subscriptions: Dict[str, Subscription] = None
    ):

        self.host = host
        self.port = port
        self.ssl_context = ssl_context
        self.client_id = client_id
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
        headers = OrderedDict({
            HDR_ACK: subscription.ack,
            HDR_DESTINATION: subscription.destination,
            HDR_ID: subscription.id,
        })
        headers.update(subscription.extra_headers)

        self._protocol.send_frame(CMD_SUBSCRIBE, headers)

    def unsubscribe(self, subscription: Subscription) -> None:
        if self._protocol is None:
            raise RuntimeError("Not connected")
        headers = OrderedDict({
            HDR_ID: subscription.id,
            HDR_DESTINATION: subscription.destination
        })
        self._protocol.send_frame(CMD_UNSUBSCRIBE, headers)

    def send(self, headers: _OrderedDict[str, Any], body: Union[bytes, str]) -> None:
        if self._protocol is None:
            raise RuntimeError("Not connected")
        self._protocol.send_frame(CMD_SEND, headers, body)

    def ack(self, frame: Frame) -> None:
        if self._protocol is None:
            raise RuntimeError("Not connected")
        headers = OrderedDict({
            HDR_SUBSCRIPTION: frame.headers[HDR_SUBSCRIPTION],
            HDR_MESSAGE_ID: frame.headers[HDR_MESSAGE_ID],
        })
        self._protocol.send_frame(CMD_ACK, headers)

    def nack(self, frame: Frame) -> None:
        if self._protocol is None:
            raise RuntimeError("Not connected")
        headers = OrderedDict({
            HDR_SUBSCRIPTION: frame.headers[HDR_SUBSCRIPTION],
            HDR_MESSAGE_ID: frame.headers[HDR_MESSAGE_ID],
        })
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
        subscription = self.subscriptions[frame.headers[HDR_SUBSCRIPTION]]
        with AutoAckContextManager(
            self, ack_mode=subscription.ack, enabled=subscription.auto_ack
        ) as ack_context:
            ack_context.frame = frame
            ack_context.result = True

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

        self._frames: Deque[bytes] = deque()

        self._transport: Optional[asyncio.Transport] = None
        self._protocol = AmqProtocol()
        self._connect_headers: _OrderedDict[str, str] = OrderedDict()

        self._connect_headers[HDR_ACCEPT_VERSION] = "1.1"

        if client_id is not None:
            unique_id = uuid.uuid4()
            self._connect_headers["client-id"] = f"{client_id}-{unique_id}"

        if self.heartbeat.get("enabled"):
            self._connect_headers[HDR_HEARTBEAT] = "{},{}".format(
                self.heartbeat.get("cx", 0), self.heartbeat.get("cy", 0)
            )

        if username is not None:
            self._connect_headers["login"] = username

        if password is not None:
            self._connect_headers["passcode"] = password

    def close(self) -> None:
        # Close the transport only if already connection is made
        if self._transport:
            # Close the transport to stomp receiving any more data
            self._transport.close()

        if self.heartbeater:
            self.heartbeater.shutdown()
            self.heartbeater = None

    def connect(self) -> None:
        buf = self._protocol.build_frame(
            CMD_CONNECT, headers=self._connect_headers)
        if not self._transport:
            raise StompDisconnectedError()
        self._transport.write(buf)
        print(buf)

    def send_frame(self, command: str, headers: Optional[_OrderedDict[str, Any]] = None,
                   body: Union[str, bytes] = b"",) -> None:
        if headers is None:
            headers = OrderedDict()
        buf = self._protocol.build_frame(command, headers, body)

        if not self._transport:
            raise StompDisconnectedError()

        if self._stats:
            self._stats.increment("sent_msg")

        self._transport.write(buf)
        print(buf)

    def connection_made(self, transport: asyncio.Transport) -> None:
        logger.info("Connected")
        self._transport = transport

        self.connect()

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
        print("hello")
        heartbeat = frame.headers.get("heart-beat")
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

    async def _handle_error(self, frame: Frame) -> None:
        message = frame.headers.get("message")

        logger.error("Received error: %s" % message)
        logger.debug("Error details: %s" % frame.body)

    async def _handle_exception(self, frame: Frame) -> None:
        logger.warning("Unhandled frame: %s", frame.command)

    def data_received(self, data: Optional[bytes]) -> None:
        print("1111")
        print(data)
        if not data:
            return
        self._protocol.process_data(data)
        for frame in self._protocol.pop_frames():
            if frame.command == CONNECTED:
                handle = self.handlers_map.get(frame.command)
                if handle:
                    print(frame)
                    yield from handle(frame).__await__()
            self._frame_handler.feed_data(frame)

    def eof_received(self) -> None:
        self.connection_lost(Exception("Got EOF from server"))
