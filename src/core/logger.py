import logging
import asyncio
import sys

def setup_logger(name) -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.hasHandlers():
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        fmt = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%H:%M:%S"
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger

class AsyncLogHandler(logging.Handler):
    def __init__(self, queue):
        super().__init__()
        self.queue = queue
        self.loop = asyncio.get_running_loop()

    def emit(self, record):
        try:
            msg = self.format(record)
            self.loop.call_soon_threadsafe(self.queue.put_nowait, msg)
        except Exception:
            self.handleError(record)