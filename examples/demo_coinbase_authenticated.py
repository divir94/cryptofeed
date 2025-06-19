import logging

from cryptofeed import FeedHandler
from cryptofeed.defines import L2_BOOK, TRADES, BID, ASK
from cryptofeed.exchanges import Coinbase
from cryptofeed.log import get_logger

LOG = get_logger(__name__, "feedhandler.log", logging.INFO)


async def trade(t, receipt_timestamp):
    LOG.info(f"Trade received at {receipt_timestamp}: {t}")


async def book(book, receipt_timestamp):
    LOG.info(f'Book received at {receipt_timestamp} for {book.exchange} - {book.symbol}, with {len(book.book)} entries. Top of book prices: {book.book.asks.index(0)[0]} - {book.book.bids.index(0)[0]}')
    if book.delta:
        LOG.info(f"Delta from last book contains {len(book.delta[BID]) + len(book.delta[ASK])} entries.")
    if book.sequence_number:
        assert isinstance(book.sequence_number, int)


def main():
    config = 'config.yaml'
    coinbase = Coinbase(
        config=config,
        symbols=['BTC-USD'],
        channels=[L2_BOOK, TRADES],
        callbacks={L2_BOOK: book, TRADES: trade},
        timeout=5,
        retries=0,
    )
    f = FeedHandler(config=config)
    f.add_feed(coinbase)
    f.run()


if __name__ == '__main__':
    main()
