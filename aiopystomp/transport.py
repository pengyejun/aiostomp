import re
from typing import List, Union, Dict, Any

from .base import BaseProtocol
from .constans import HEARTBEAT
from .frame import Frame
from .utils import _unescape_header, decode, encode


class Amq:
    V1_0 = "1.0"
    V1_1 = "1.1"
    V1_2 = "1.2"


class AmqProtocol(BaseProtocol):
    EOF = b"\x00"
    HEART_BEAT = b"\n"
    END_LINE = b"\n"
    PREAMBLE_END_RE = re.compile(b"\n\n|\r\n\r\n")
    HEADER_LINE_RE = re.compile("(?P<key>[^:]+)[:](?P<value>.*)")
    LINE_END_RE = re.compile("\n|\r\n")
    CONTENT_LENGTH_RE = re.compile(b"^content-length[:]\\s*(?P<value>[0-9]+)", re.MULTILINE)
    HeartBeatFrame = Frame(HEARTBEAT)
    HEADER_MAP = {"\n": "\\n", ":": "\\c", "\\": "\\\\", "\r": "\\r"}
    _version = Amq.V1_1

    def process_data(self, data: bytes):
        if data is None or len(data) == 0:
            return

        if data == self.HEART_BEAT:
            data += self.EOF

        self._recvbuf += data
        self.consume_data()

    def consume_data(self):
        while True:
            pos = self._recvbuf.find(self.EOF)
            if pos < 0:
                return
            frame = self._recvbuf[:pos]
            preamble_end_match = self.PREAMBLE_END_RE.search(frame)
            if preamble_end_match:
                preamble_end = preamble_end_match.start()
                content_length_match = self.CONTENT_LENGTH_RE.search(frame[0:preamble_end])
                if content_length_match:
                    content_length = int(content_length_match.group("value"))
                    content_offset = preamble_end_match.end()
                    frame_size = content_offset + content_length
                    if frame_size > len(frame):
                        # Frame contains NUL bytes, need to read more
                        if frame_size >= len(self._recvbuf):
                            # Haven't read enough data yet, exit loop and wait for more to arrive
                            return
                        pos = frame_size
                        frame = self._recvbuf[0:pos]
            frame = self.parse_frame(frame)
            if frame is not None:
                self.append_frame(frame)
            pos += 1
            while self._recvbuf[pos:pos + 1] == self.END_LINE:
                pos += 1
            self._recvbuf = self._recvbuf[pos:]

    def parse_frame(self, data: bytes) -> Union[Frame, None]:
        if data == self.HEART_BEAT:
            return self.HeartBeatFrame
        mat = self.PREAMBLE_END_RE.search(data)
        if mat:
            preamble_end = mat.start()
            body_start = mat.end()
        else:
            preamble_end = len(data)
            body_start = preamble_end
        preamble = decode(data[0:preamble_end])
        preamble_lines = self.LINE_END_RE.split(preamble)
        preamble_len = len(preamble_lines)

        # Skip any leading newlines
        first_line = 0
        while first_line < preamble_len and len(preamble_lines[first_line]) == 0:
            first_line += 1

        if first_line >= preamble_len:
            return None
        f = Frame(preamble_lines[first_line])
        f.body = decode(data[body_start:]) if self.auto_decode else data[body_start:]
        f.headers = self.parse_headers(preamble_lines, first_line + 1)
        return f

    def parse_headers(self, lines: List[str], offset: int = 0) -> Dict[str, str]:
        headers = {}
        for header_line in lines[offset:]:
            header_match = self.HEADER_LINE_RE.match(header_line)
            if header_match:
                key = header_match.group("key")
                key = re.sub(r'\\.', _unescape_header, key)
                if key not in headers:
                    value = header_match.group("value")
                    value = re.sub(r'\\.', _unescape_header, value)
                    headers[key] = value
        return headers

    def build_frame(self, command: str, headers: Dict[str, Any], body: Union[bytes, str] = "") -> bytes:
        lines: List[Union[str, bytes]] = [command, "\n"]

        for key, value in headers.items():
            lines.append(f"{key}:{self._encode_header(value)}\n")

        lines.append("\n")
        lines.append(body)
        lines.append(self.EOF)

        return b"".join(encode(line) for line in lines)

    def _encode_header(self, header_value: Any) -> str:
        value = f"{header_value}"
        if self._version == Amq.V1_0:
            return value
        return "".join(self.HEADER_MAP.get(c, c) for c in value)
