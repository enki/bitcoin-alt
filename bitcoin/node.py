import threading
import queue
import random

import bitcoin.net.payload

import bitcoin.net.peer
import bitcoin.storage

class Node(threading.Thread):
  def __init__(self,cb,peers,shutdown):
    super(Node,self).__init__()
    
    self.cb = cb
    self.peers = peers
    
    self.shutdown = shutdown
    self.daemon = True
    
  def run(self):
    self.storage = bitcoin.storage.Storage()# this has to be here so that it's created in the same thread as it's used
    
    while True:
      try:
        event = self.cb.get(True,0.1)
        
        peer = event['peer']
        command = event['command']
        payload = event['payload']
        
        {'addr':self.handle_addr,
        'inv':self.handle_inv,
        'getdata':self.handle_getdata,
        'getblocks':self.handle_getblocks,
        'getheaders':self.handle_getheaders,
        'tx':self.handle_tx,
        'block':self.handle_block,
        'getaddr':self.handle_getaddr,
        'alert':self.handle_alert,
        }[command](peer,payload)
      except queue.Empty as e:
        pass
      except KeyboardInterrupt as e:
        return
      finally:
        if self.shutdown.is_set():
          return
      
  def handle_addr(self,peer,payload):
    for addr in payload['addrs']:
      self.peers.add((addr['node_addr']['addr'],addr['node_addr']['port']))
  
  def handle_inv(self,peer,payload):
    invs = []
    for inv in payload['invs']:
      if inv['type'] == 1:
        if not self.storage.get_tx(inv['hash']):
          invs.append(inv)
          print(inv)
      if inv['type'] == 2:
        if not self.storage.get_block(inv['hash']):
          invs.append(inv)
    peer.send_getdata(invs)
          
  
  def handle_getdata(self,peer,payload):
    for inv in payload['invs']:
      if inv['type'] == 1:
        d = self.storage.get_tx(inv['hash'])
        if d:
          peer.send_tx(d)
      if inv['type'] == 2:
        d = self.storage.get_block(inv['hash'])
        if d:
          peer.send_block(d)
  
  def handle_getblocks(self,peer,payload):
    pass
    
  def handle_getheaders(self,peer,payload):
    pass
    
  def handle_tx(self,peer,payload):
    self.storage.put_tx(payload)
    
  def handle_block(self,peer,payload):
    self.storage.put_block(payload)
    
  def handle_headers(self,peer,payload):
    pass
    
  def handle_getaddr(self,peer,payload):
    pass
    
  def handle_checkorder(self,peer,payload):
    pass
    
  def handle_submitorder(self,peer,payload):
    pass
    
  def handle_reply(self,peer,payload):
    pass
    
  def handle_alert(self,peer,payload):
    pass


