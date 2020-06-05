import asyncio
from abc import abstractmethod
from typing import Optional, List, Any, Union, Dict

from .frame import Frame

from .subscription import Subscription


class StompBaseProtocol:

    @abstractmethod
    async def connect(self, username: Optional[str] = None, password: Optional[str] = None) -> None:
        """
        连接socket
        """

    @abstractmethod
    def close(self) -> None:
        """
        关闭socket
        """

    @abstractmethod
    def subscribe(self, subscription: Subscription) -> None:
        """
        订阅操作
        """

    @abstractmethod
    def unsubscribe(self, subscription: Subscription) -> None:
        """
        取消订阅
        """

    @abstractmethod
    def ack(self, frame: Frame) -> None:
        """
        确认机制
        """

    @abstractmethod
    def nack(self, frame: Frame) -> None:
        """
        否定应答
        """

    @abstractmethod
    def feed_data(self, frame: Frame):
        """
        处理frame
        """

    @abstractmethod
    def connection_lost(self, exc: Optional[Exception]) -> None:
        """
        连接关闭处理
        """


class ConnectionListener(object):
    """
    This class should be used as a base class for objects registered
    using Connection.set_listener().
    """
    def on_connecting(self, host_and_port):
        """
        Called by the STOMP connection once a TCP/IP connection to the
        STOMP server has been established or re-established. Note that
        at this point, no connection has been established on the STOMP
        protocol level. For this, you need to invoke the "connect"
        method on the connection.

        :param (str,int) host_and_port: a tuple containing the host name and port number to which the connection
            has been established.
        """
        pass

    def on_connected(self, frame: Frame):
        """
        Called by the STOMP connection when a CONNECTED frame is
        received (after a connection has been established or
        re-established).

        :param Frame frame: the Frame message
        """
        pass

    def on_disconnected(self):
        """
        Called by the STOMP connection when a TCP/IP connection to the
        STOMP server has been lost.  No messages should be sent via
        the connection until it has been reestablished.
        """
        pass

    def on_heartbeat_timeout(self):
        """
        Called by the STOMP connection when a heartbeat message has not been
        received beyond the specified period.
        """
        pass

    def on_before_message(self, frame: Frame):
        """
        Called by the STOMP connection before a message is returned to the client app. Returns a tuple
        containing the headers and body (so that implementing listeners can pre-process the content).

        :param Frame frame: the Frame message
        """
        return frame

    async def on_message(self, frame: Frame):
        """
        Called by the STOMP connection when a MESSAGE frame is received.

        :param Frame frame: the Frame message
        """
        pass

    def on_receipt(self, frame: Frame):
        """
        Called by the STOMP connection when a RECEIPT frame is
        received, sent by the server if requested by the client using
        the 'receipt' header.

        :param Frame frame: the Frame message
        """
        pass

    async def on_error(self, frame: Frame):
        """
        Called by the STOMP connection when an ERROR frame is received.

        :param Frame frame: the Frame message
        """
        pass

    def on_send(self, frame: Frame):
        """
        Called by the STOMP connection when it is in the process of sending a message

        :param Frame frame: the frame to be sent
        """
        pass

    def on_heartbeat(self, frame: Frame):
        """
        Called on receipt of a heartbeat.
        """
        pass

    def on_receiver_loop_completed(self, frame: Frame):
        """
        Called when the connection receiver_loop has finished.
        :param Frame frame: the Frame message
        """
        pass


class Publisher(object):
    """
    Simply a registry of listeners.
    """

    @abstractmethod
    def set_listener(self, name: str, listener: ConnectionListener) -> None:
        """
        Set a named listener to use with this connection. See :py:class:`stomp.listener.ConnectionListener`

        :param str name: the name of the listener
        :param ConnectionListener listener: the listener object
        """

    @abstractmethod
    def remove_listener(self, name: str) -> None:
        """
        Remove a listener.

        :param str name: the name of the listener to remove
        """

    @abstractmethod
    def get_listener(self, name: str) -> ConnectionListener:
        """
        Return the named listener.

        :param str name: the listener to return

        :rtype: ConnectionListener
        """


class BaseProtocol:

    def __init__(self, auto_decode=True):
        self._recvbuf = b""
        self.auto_decode = auto_decode
        self.__ready_frames: List[Frame] = []

    @abstractmethod
    def process_data(self, data: bytes):
        """
        process data
        """

    @abstractmethod
    def consume_data(self):
        """
        process recvbuf
        """

    @abstractmethod
    def parse_frame(self, data: bytes) -> Frame:
        """
        bytes -> Frame
        """

    def pop_frames(self) -> List[Frame]:
        """
            clear and return ready_frames
        """
        frames = self.__ready_frames
        self.__ready_frames = []
        return frames

    @abstractmethod
    def parse_headers(self, lines: List[str], offset: int = 0) -> Dict[str, str]:
        """
        parse headers
        """

    @abstractmethod
    def _encode_header(self, header_value: Any) -> str:
        """
        encode header
        """

    @abstractmethod
    def build_frame(self, command: str, headers: Dict[str, Any], body: Union[bytes, str] = "") -> bytes:
        """
        build frame
        """

    def append_frame(self, frame: Frame):
        self.__ready_frames.append(frame)
