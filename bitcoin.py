#!/usr/bin/env python3
import threading
import queue
import time
import sys

import bitcoin.node
import bitcoin.peers
import bitcoin.storage

static_peers = [("::ffff:174.120.185.74",8333),("::ffff:193.25.1.157",8333)]

cb = queue.Queue()
shutdown = threading.Event()
shutdown.clear()

storage = bitcoin.storage.Storage()

peers = bitcoin.peers.Peers(cb,shutdown)
peers.start()

for peer in static_peers:
  peers.add(peer)

node = bitcoin.node.Node(cb,peers,storage,shutdown)
node.start()

while True:
  try:
    time.sleep(0.1)
  except KeyboardInterrupt as e:
    shutdown.set()
    node.join()
    peers.join()
    sys.exit(0)
