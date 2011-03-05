import threading
import queue
import time
import sys

import node
import peers

static_peers = [("::ffff:10.45.134.110",8333)]

cb = queue.Queue()
shutdown = threading.Event()
shutdown.clear()

p = peers.Peers(cb,shutdown)
p.start()

for peer in static_peers:
  p.add(peer)

n = node.Node(cb,peers,shutdown)
n.start()

while True:
  try:
    time.sleep(0.1)
  except KeyboardInterrupt as e:
    shutdown.set()
    n.join()
    p.join()
    sys.exit(0)
