import threading
import time
import random

import bitcoin.storage

class Connector(threading.Thread):
  def __init__(self,storage,peermanager,shutdown):
    super(Connector,self).__init__()
    
    self.storage = storage
    self.peermanager = peermanager
    self.shutdown = shutdown
    
    self.daemon = True
  
  def run(self):
    while True:
      self.connect_blocks()
      if self.shutdown.is_set():
        return
      else:
        time.sleep(0.5)
      
  def connect_blocks(self):
    print("connect_blocks")
    genesis_block = self.storage.get_block(bitcoin.storage.genesis_hash)
    if not genesis_block:
      open_peers = self.peermanager.open()
      for peer in open_peers:
        self.peermanager.get_thread(peer).send_getdata([{'type':2,'hash':bitcoin.storage.genesis_hash}])
    
    print("get_heads")
    heads = self.storage.get_heads()
    ends = []
    print("calculating height...")
    while heads:
      head = heads.pop()
      print("head ",head.hash)
      if head.next_blocks:
        s=time.time()
        for next_block in head.next_blocks:
          next_block.height = head.height + next_block.difficulty()
          self.storage.put_block(next_block)
          heads.append(next_block)
        e=time.time()
        if e-s > 1:
          print(e-s)
      else:
        print("found true head... ",head.hash)
        ends.append(head)
    
    if ends:
      open_peers = self.peermanager.open()
      for peer in random.sample(open_peers,min(3  ,len(open_peers))):
        self.peermanager.get_thread(peer).send_getblocks([block.hash for block in ends])
    print("connect_blocks end")
