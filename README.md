# aiostomp.
对stomp的简单异步封装，实验项目，生产环境慎用

## Install

with pip:

```bash
pip install aiostomp-py
```
## Usage
```python
import os
import uvloop
import logging
import asyncio

from aiopystomp.frame import Frame

from aiopystomp import AioStomp

from aiopystomp.base import ConnectionListener


os.environ["AIOSTOMP_ENABLE_STATS"] = "1"


logging.basicConfig(
    format="%(asctime)s - %(filename)s:%(lineno)d - "
    "%(levelname)s - %(message)s",
    level='INFO')


class Listener(ConnectionListener):

    def on_connected(self, frame: Frame):
        pass

    def on_heartbeat(self, frame: Frame):
        pass

    async def on_message(self, frame: Frame):
        pass
        # print(frame)
        # frame.body = ujson.loads(frame.body)
        # print(self.index)

    async def on_error(self, frame: Frame):
        pass


body = '{"seq":"collector1-1591240680008","tid":"patrol_policy_2_2bd91c24-47af-4cbf-a01a-62169cb30003_useable_state",' \
       '"tm":1591240692017,"cost":12014,"ne":"2bd91c24-47af-4cbf-a01a-62169cb30003","ind":"useable_state","v":false,' \
       '"type":"Value"}'


async def send(client):
    i = 1000000
    while i > 0:
        client.send('/queue/channel', body=f"{body}")
        if i % 200000 == 0:
            await asyncio.sleep(0)


async def run():
    client = AioStomp("127.0.0.1", 61613)
    await client.connect(username="admin", password="admin")
    client.subscribe("/queue/channel")
    l = Listener()
    l1 = Listener()
    client.set_listener("abc", l)
    client.set_listener("abc1", l1)
    # await send(client)


def main():
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run())
    loop.run_forever()


if __name__ == '__main__':
    main()


```
