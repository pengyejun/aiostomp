import sys
import logging
import asyncio

from aiostomp import AioStomp
import stomp

logging.basicConfig(
    format="%(asctime)s - %(filename)s:%(lineno)d - "
    "%(levelname)s - %(message)s",
    level='DEBUG')


async def run():
    client = AioStomp('localhost', 61616, error_handler=report_error)
    client.subscribe('/queue/channel', handler=on_message)

    await client.connect()

    client.send('/queue/channel', body=u'Thanks', headers={})


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