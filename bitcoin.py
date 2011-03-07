#!/usr/bin/env python3
import threading
import queue
import time
import sys

import bitcoin.node
import bitcoin.peers

#static_peers = [("::ffff:174.120.185.74",8333),("::ffff:193.25.1.157",8333)]
static_peers = [("::ffff:10.45.134.110",8333)]

cb = queue.Queue()
shutdown = threading.Event()
shutdown.clear()

peers = bitcoin.peers.Peers(cb,shutdown)
peers.start()

for peer in static_peers:
  peers.add(peer)

node = bitcoin.node.Node(cb,peers,shutdown)
node.start()

while True:
  try:
    time.sleep(0.1)
  except KeyboardInterrupt as e:
    shutdown.set()
    node.join()
    peers.join()
    sys.exit(0)
