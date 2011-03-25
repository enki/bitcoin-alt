#!/usr/bin/env python3
import threading
import queue
import time
import sys

import bitcoin.peermanager
import bitcoin.storage
import bitcoin.connector

static_peers = [("::ffff:174.120.185.74",8333)]#,("::ffff:193.25.1.157",8333)]
#static_peers = [("::ffff:10.45.134.110",8333)]

shutdown = threading.Event()
shutdown.clear()

storage = bitcoin.storage.Storage(shutdown)
storage.start()

peers = bitcoin.peermanager.PeerManager(storage,shutdown,1)
peers.start()

connector = bitcoin.connector.Connector(storage,peers,shutdown)
connector.start()

for peer in static_peers:
  peers.add(peer)

while True:
  try:
    time.sleep(0.1)
  except KeyboardInterrupt as e:
    shutdown.set()
    peers.join()
    sys.exit(0)
