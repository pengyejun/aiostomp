import asyncio
import os
from typing import Dict, List
from .log import logger


class AioStompStats:
    def __init__(self) -> None:
        self.connection_count = 0
        self.interval = int(os.environ.get("AIOSTOMP_STATS_INTERVAL", 10))
        self.connection_stats: List[Dict[str, int]] = []

    def print_stats(self) -> None:
        logger.info("==== AioStomp Stats ====")
        logger.info("Connections count: {}".format(self.connection_count))
        logger.info(" con | sent_msg | rec_msg ")
        for index, stats in enumerate(self.connection_stats):
            logger.info(
                " {:>3} | {:>8} | {:>7} ".format(
                    index + 1, stats["sent_msg"], stats["rec_msg"]
                )
            )
        logger.info("========================")

    def new_connection(self) -> None:
        self.connection_stats.insert(0, {"sent_msg": 0, "rec_msg": 0})

        if len(self.connection_stats) > 5:
            self.connection_stats.pop()

    def increment(self, field: str) -> None:
        if len(self.connection_stats) == 0:
            self.new_connection()

        if field not in self.connection_stats[0]:
            self.connection_stats[0][field] = 1
            return

        self.connection_stats[0][field] += 1

    async def run(self) -> None:
        while True:
            await asyncio.sleep(self.interval)
            self.print_stats()
