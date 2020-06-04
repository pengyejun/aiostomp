from typing import Union

_HEADER_ESCAPES = {
    '\r': '\\r',
    '\n': '\\n',
    ':': '\\c',
    '\\': '\\\\',
}
_HEADER_UNESCAPES = dict((value, key) for (key, value) in _HEADER_ESCAPES.items())


def _unescape_header(matchobj):
    escaped = matchobj.group(0)
    unescaped = _HEADER_UNESCAPES.get(escaped)
    if not unescaped:
        # TODO: unknown escapes MUST be treated as fatal protocol error per spec
        unescaped = escaped
    return unescaped


def encode(value: Union[str, bytes]) -> bytes:
    if isinstance(value, str):
        return value.encode("utf-8")

    return value


def decode(byte_data: bytes, encoding="utf-8") -> Union[str, None]:
    if byte_data is None:
        return None
    return byte_data.decode(encoding)
