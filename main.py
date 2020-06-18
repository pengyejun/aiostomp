import os
import time
import traceback

import ujson
import uvloop
import logging
import asyncio
from asyncio import Queue
from typing import List

from aioelasticsearch import Elasticsearch, RequestError

from aiopystomp.frame import Frame

from aiopystomp import AioStomp

from aiopystomp.base import ConnectionListener


# os.environ["AIOSTOMP_ENABLE_STATS"] = "1"


logging.basicConfig(
    format="%(asctime)s - %(filename)s:%(lineno)d - "
    "%(levelname)s - %(message)s",
    level='INFO')


class ESUtils(object):
    @classmethod
    async def create_index_by_analyzer(cls, _es: Elasticsearch, index: str, analyzer: str = "standard"):
        """
            创建索引, 指定分词器
        """
        await _es.indices.create(index, {
            "settings": {
                "index": {
                    "analysis.analyzer.default.type": analyzer
                }
            }
        })

    @staticmethod
    async def get_index(_es: Elasticsearch) -> list:
        """获取es中所有的索引"""
        exist_indexes = await _es.indices.get_alias("*")
        return list(exist_indexes.keys())


class Listener(ConnectionListener):

    def __init__(self, es: Elasticsearch, exist_indexes: List):
        self.es = es
        self.indices = exist_indexes
        self.t = time.time()
        self.index = 0

    async def _judge_index_name(self, index_name):
        if index_name not in self.indices:
            # 设置分词器为 keyword
            self.indices = await ESUtils.get_index(self.es)
            if index_name not in self.indices:
                await ESUtils.create_index_by_analyzer(self.es, index_name, "keyword")
                self.indices.append(index_name)

    def on_connected(self, frame: Frame):
        pass

    def on_heartbeat(self, frame: Frame):
        print(frame)

    async def on_message(self, frame: Frame):
        data = ujson.loads(body)
        data['run_status'] = "Good"  # 原monitor数据库有此字段，java后端获取数据希望加上
        try:
            index_name = data.get('ind').lower()
            # 1. 创建索引
            await self._judge_index_name(index_name)
            await self._judge_index_name("current_" + index_name)
            # 2. 插入数据
            v: List[dict] = data.get('v')
            await self.es.index(index=index_name, body=data)
            current_id = data.get("ne")  # 创建更新实时数据
            if isinstance(v, list) and len(v) != 0:
                data["v"] = str(v)
            await self.es.index(index="current_" + index_name, body=data, id=current_id)

        except RequestError:
            logging.error(traceback.format_exc())
        self.index += 1
        if self.index % 200000 == 0:
            print(time.time() - self.t)
            self.t = time.time()

    async def on_error(self, frame: Frame):
        pass


class Listener1(ConnectionListener):

    def __init__(self, q: Queue):
        self.q = q

    def on_connected(self, frame: Frame):
        pass

    def on_heartbeat(self, frame: Frame):
        self.q.put_nowait(b"\n")

    async def on_message(self, frame: Frame):
        for i in range(2000):
            await self.q.put(frame.body)

    async def on_error(self, frame: Frame):
        pass


body = '{"seq":"collector1-1591240680008","tid":"patrol_policy_2_2bd91c24-47af-4cbf-a01a-62169cb30003_useable_state",' \
       '"tm":1591240692017,"cost":12014,"ne":"2bd91c24-47af-4cbf-a01a-62169cb30003","ind":"useable_state","v":false,' \
       '"type":"Value"}'


async def send(client, q: Queue):
    while True:
        msg = await q.get()
        client.send('/queue/channel', body=msg)


async def run():
    # async with Elasticsearch(hosts='http://127.0.0.1:9200') as es:
    client = AioStomp("192.168.1.197", 61613, auto_decode=False)
    await client.connect(username="admin", password="admin")
    client1 = AioStomp("127.0.0.1", 61613, auto_decode=False)
    await client1.connect(username="admin", password="admin")
    client.subscribe("/topic/SIMO_IND_V_TO_RULE")
    # exist_indexes = await ESUtils.get_index(es)
    # l = Listener(es, exist_indexes)
    q = Queue()
    l = Listener1(q)
    client.set_listener("abc", l)
    # client.set_listener("abc1", l1)
    await send(client1, q)


def main():
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run())
    loop.run_forever()


if __name__ == '__main__':
    main()
