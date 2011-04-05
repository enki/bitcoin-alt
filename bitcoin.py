#!/usr/bin/env python3

import logging
import sys
import threading
import time

import bitcoin.peers

static_peers = [("::ffff:174.120.185.74",8333),("::ffff:193.25.1.157",8333)]

logging.basicConfig()
logger = logging.getLogger('')
if '-d' in sys.argv[1:]:
  logger.setLevel(logging.INFO)
if '-dd' in sys.argv[1:]:
  logger.setLevel(logging.DEBUG)

shutdown = threading.Event()
shutdown.clear()

storage = bitcoin.storage.Storage()

peers = bitcoin.peers.Peers(shutdown,storage,1)
peers.start()

for peer in static_peers:
  logger.info('Adding peer %s', peer)
  peers.add(peer)

while True:
  try:
    time.sleep(0.1)
  except KeyboardInterrupt:
    logger.info('Sending the shutdown event')
    shutdown.set()
    peers.join()
    sys.exit(0)
