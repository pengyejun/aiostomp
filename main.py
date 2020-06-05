import sys
import uvloop
import time
import logging
import asyncio
import aioelasticsearch
import ujson

from aiostomp.frame import Frame

from aiostomp import AioStomp

from aiostomp.base import ConnectionListener

logging.basicConfig(
    format="%(asctime)s - %(filename)s:%(lineno)d - "
    "%(levelname)s - %(message)s",
    level='INFO')


class Listener(ConnectionListener):

    def __init__(self):
        self.index = 0

    def on_connected(self, frame: Frame):
        pass

    def on_heartbeat(self, frame: Frame):
        pass

    async def on_message(self, frame: Frame):
        frame.body = ujson.loads(frame.body)
        print(frame)

    async def on_error(self, frame: Frame):
        pass


body = '{"seq":"collector1-1591240680008","tid":"patrol_policy_2_2bd91c24-47af-4cbf-a01a-62169cb30003_useable_state",' \
       '"tm":1591240692017,"cost":12014,"ne":"2bd91c24-47af-4cbf-a01a-62169cb30003","ind":"useable_state","v":false,' \
       '"type":"Value"}'


async def send(client):
    t1 = time.time()
    i = 1000000
    while i > 0:
        client.send('/queue/channel', body=f"{body}")
        if i % 200000 == 0:
            await asyncio.sleep(0)
        i -= 1
    print(time.time() - t1)


async def run():
    client = AioStomp("127.0.0.1", 61613)
    await client.connect(username="admin", password="admin")
    client.subscribe("/queue/channel")
    l = Listener()
    client.set_listener("abc", l)
    # await send(client)


def main(args):
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run())
    loop.run_forever()


if __name__ == '__main__':
    main(sys.argv)
