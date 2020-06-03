import logging
from collections import deque
from typing import List, Optional, Dict, Deque, Union, Any
from unittest import TestCase

from aiostomp.constans import CMD_SEND, MESSAGE, ERROR
from aiostomp.frame import Frame
from aiostomp.log import logger
from aiostomp.protocol import ends_with_crlf


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
        print(data)
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
                        Frame("HEARTBEAT", headers={}, body=None))
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
protocol: AmqProtocol = AmqProtocol()

message = b'CONNECTED\nserver:ActiveMQ/5.13.0\nheart-beat:1000,1000\nsession:ID:localhost.localdomain-43464-1591069057460-3:315\nversion:1.1\n\n\x00\n'
protocol.process_data(message)
print("1234")
# class TestAmqProtocol(TestCase):
#     def setUp(self):
#         self.protocol: AmqProtocol = AmqProtocol()
#
#     def test_parse_message(self):
#         message = b'CONNECTED\nserver:ActiveMQ/5.13.0\nheart-beat:1000,1000\nsession:ID:localhost.localdomain-43464-1591069057460-3:315\nversion:1.1\n\n\x00\n'
#         self.protocol.process_data(message)