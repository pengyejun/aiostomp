import time
from unittest import TestCase

from ..protocol import AmqProtocol


class TestAmqProtocol(TestCase):
    def setUp(self):
        self.protocol: AmqProtocol = AmqProtocol()

    def test_parse_message(self):
        t1 = time.time()
        message = b'CONNECTED\nserver:ActiveMQ/5.13.0\nheart-beat:1000,1000\nsession:ID:localhost.localdomain-43464-1591069057460-3:315\nversion:1.1\n\n\x00\n'
        for i in range(1000000):
            self.protocol.process_data(message)
        print(time.time() - t1)
