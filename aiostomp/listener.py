class Publisher(object):
    """
    Simply a registry of listeners.
    """

    def set_listener(self, name, listener):
        """
        Set a named listener to use with this connection. See :py:class:`stomp.listener.ConnectionListener`

        :param str name: the name of the listener
        :param ConnectionListener listener: the listener object
        """
        pass

    def remove_listener(self, name):
        """
        Remove a listener.

        :param str name: the name of the listener to remove
        """
        pass

    def get_listener(self, name):
        """
        Return the named listener.

        :param str name: the listener to return

        :rtype: ConnectionListener
        """
        return None


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

    def on_connected(self, headers, body):
        """
        Called by the STOMP connection when a CONNECTED frame is
        received (after a connection has been established or
        re-established).

        :param dict headers: a dictionary containing all headers sent by the server as key/value pairs.
        :param body: the frame's payload. This is usually empty for CONNECTED frames.
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

    def on_before_message(self, headers, body):
        """
        Called by the STOMP connection before a message is returned to the client app. Returns a tuple
        containing the headers and body (so that implementing listeners can pre-process the content).

        :param dict headers: the message headers
        :param body: the message body
        """
        return headers, body

    async def on_message(self, headers, body):
        """
        Called by the STOMP connection when a MESSAGE frame is received.

        :param dict headers: a dictionary containing all headers sent by the server as key/value pairs.
        :param body: the frame's payload - the message body.
        """
        pass

    def on_receipt(self, headers, body):
        """
        Called by the STOMP connection when a RECEIPT frame is
        received, sent by the server if requested by the client using
        the 'receipt' header.

        :param dict headers: a dictionary containing all headers sent by the server as key/value pairs.
        :param body: the frame's payload. This is usually empty for RECEIPT frames.
        """
        pass

    def on_error(self, headers, body):
        """
        Called by the STOMP connection when an ERROR frame is received.

        :param dict headers: a dictionary containing all headers sent by the server as key/value pairs.
        :param body: the frame's payload - usually a detailed error description.
        """
        pass

    def on_send(self, frame):
        """
        Called by the STOMP connection when it is in the process of sending a message

        :param Frame frame: the frame to be sent
        """
        pass

    def on_heartbeat(self):
        """
        Called on receipt of a heartbeat.
        """
        pass

    def on_receiver_loop_completed(self, headers, body):
        """
        Called when the connection receiver_loop has finished.
        """
        pass
