import threading
import random
import time

import bitcoin.net.peer

class Peers:
  def __init__(self,cb):
    self.peers = {}
    self.plock = threading.RLock()
    
    self.cb = cb
    
    self.shutdown = threading.Event()
    self.shutdown.clear()
  
  def add(self,address):
    with self.plock:
      if address not in self.peers:
        self.peers[address] = {}
        self.peers[address]['last_tried'] = 0 # we can assume that we're running this program after 1/1/1970
        self.peers[address]['thread'] = None
        
  def start(self,address):
    with self.plock:
      self.add(address)
      if not self.peers[address]['thread'] or not self.peers[address]['thread'].is_alive():# TODO race condition if anybody else is starting peers, per peer lock?
        self.peers[address]['thread'] = bitcoin.net.peer.Peer(address,self.cb,self.shutdown)
        self.peers[address]['thread'].start()
  
  def get_random(self,count=1):
    peers = []
    with self.plock:
      peers.append(random.choice(self.peers))
    return peers
    
  def start_minimum(self,count=8):
    open_peers = self.open()
    closed_peers = self.closed()

    while len(open_peers) < count and len(closed_peers) > 0:
      peer = random.choice(closed_peers)
      if self.peers[peer]['last_tried'] + 5 * 60 < time.time(): # TODO arbitrary constant (tried more than 5 minutes ago)
        self.peers[peer]['last_tried'] = time.time()
        self.start(peer)
      closed_peers.remove(peer)
  
  def closed(self):
    ret = []
    for peer in self.peers:
      if not self.peers[peer]['thread'] or not self.peers[peer]['thread'].is_alive():# TODO race condition if anybody else is starting peers, per peer lock?
        ret.append(peer)
    return ret
  
  def open(self):
    ret = []
    for peer in self.peers:
      if self.peers[peer]['thread'] and self.peers[peer]['thread'].is_alive():# TODO race condition if anybody else is starting peers, per peer lock?
        ret.append(peer)
    return ret
