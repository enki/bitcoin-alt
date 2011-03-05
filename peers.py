import threading
import random

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
        self.peers[address] = None
        
  def start(self,address):
    with self.plock:
      self.add(address)
      if not self.peers[address] or not self.peers[address].is_alive():# TODO race condition if anybody else is starting peers, per peer lock?
        self.peers[address] = bitcoin.net.peer.Peer(address,self.cb,self.shutdown)
        self.peers[address].start()
  
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
      self.start(peer)
      closed_peers.remove(peer)
  
  def closed(self):
    ret = []
    for peer in self.peers:
      if not self.peers[peer] or not self.peers[peer].is_alive():# TODO race condition if anybody else is starting peers, per peer lock?
        ret.append(peer)
    return ret
  
  def open(self):
    ret = []
    for peer in self.peers:
      if self.peers[peer] and self.peers[peer].is_alive():# TODO race condition if anybody else is starting peers, per peer lock?
        ret.append(peer)
    return ret
