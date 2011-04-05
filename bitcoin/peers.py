import threading
import random
import time

import bitcoin.peer

class Peers(threading.Thread):
  def __init__(self,shutdown,storage,count=8):
    super(Peers,self).__init__()
    
    self.storage = storage
    
    self.peers = {}
    self.count = count
    self.count_lock = threading.Semaphore(count+1)
    
    self.plock = threading.RLock()
    
    self.shutdown = shutdown
    self.daemon = True
    
  def get_thread(self,address):
    with self.plock:
      if self.peers[address]['thread'].is_alive():
        return self.peers[address]['thread']
      else:
        return None
  
  def add(self,address):
    with self.plock:
      if address not in self.peers:
        self.peers[address] = {}
        self.peers[address]['last_tried'] = 0 # we can assume that we're running this program after 1/1/1970 TODO CAN WE?
        self.peers[address]['thread'] = None
        
  def start_peer(self,address):
    with self.plock:
      self.add(address)
      if not self.peers[address]['thread'] or not self.peers[address]['thread'].is_alive():# TODO race condition if anybody else is starting peers, per peer lock?
        self.peers[address]['thread'] = bitcoin.peer.Peer(address,self,self.shutdown,self.storage)
        self.peers[address]['thread'].start()
  
  def run(self):#TODO this is ridiculous inefficient
    while True:
      with self.plock:
        open_peers = self.open()
        closed_peers = self.closed()

        if len(closed_peers) > 0:
          for x in range(min(self.count-len(open_peers),len(closed_peers))):
            peer = random.choice(closed_peers)
            if self.peers[peer]['last_tried'] + 5 * 60 < time.time(): # TODO arbitrary constant (tried more than 5 minutes ago)
              self.peers[peer]['last_tried'] = time.time()
              self.start_peer(peer)
            closed_peers.remove(peer)
      
      if self.shutdown.is_set():
          return
      else:
        time.sleep(0.1)#tight loop
  
  def closed(self):
    with self.plock:
      ret = []
      for peer in self.peers:
        if not self.peers[peer]['thread'] or not self.peers[peer]['thread'].is_alive():# TODO race condition if anybody else is starting peers, per peer lock?
          ret.append(peer)
      return ret
  
  def open(self):
    with self.plock:
      ret = []
      for peer in self.peers:
        if self.peers[peer]['thread'] and self.peers[peer]['thread'].is_alive():# TODO race condition if anybody else is starting peers, per peer lock?
          ret.append(peer)
      return ret
