import threading
import queue

import node

import peers

static_peers = [("::ffff:10.45.134.110",8333)]

cb = queue.Queue()
peers = peers.Peers(cb)

for peer in static_peers:
  peers.add(peer)

n = node.Node(cb,peers)
n.run()


