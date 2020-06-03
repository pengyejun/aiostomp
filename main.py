import sys
import logging
import asyncio

import ujson

from aiostomp.frame import Frame

from aiostomp import AioStomp

from aiostomp.base import ConnectionListener

logging.basicConfig(
    format="%(asctime)s - %(filename)s:%(lineno)d - "
    "%(levelname)s - %(message)s",
    level='DEBUG')


class Listener(ConnectionListener):
    def on_connected(self, frame: Frame):
        pass

    def on_heartbeat(self, frame: Frame):
        pass

    async def on_message(self, frame: Frame):
        data = ujson.loads(frame.body)
        print(data)

    async def on_error(self, frame: Frame):
        pass


async def run():
    client = AioStomp("192.168.1.197", 61613)
    client.subscribe("/topic/SIMO_IND_V_TO_RULE")
    client.set_listener("abc", Listener())
    await client.connect(username="admin", password="admin")
    # client.send('/queue/channel', body=u'Thanks', headers={})


async def on_message(frame, message):
    print('on_message:', message)
    return True


async def report_error(error):
    print('report_error:', error)


def main(args):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
    loop.run_forever()


if __name__ == '__main__':
    main(sys.argv)