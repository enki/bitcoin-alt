import threading
import queue
import random

import bitcoin.net.peer
#import bitcoin.storage.data

class Node(threading.Thread):
  def __init__(self,cb,peers,storage,shutdown):
    super(Node,self).__init__()
    
    self.cb = cb
    self.peers = peers
    self.storage = storage
    
    self.shutdown = shutdown
    self.daemon = True
    
  def run(self):
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
    #peer.send_getdata(payload['invs'])
    pass
  
  def handle_getdata(self,peer,payload):
    for inv in payload['invs']:
      print(inv)
  
  def handle_getblocks(self,peer,payload):
    pass
    
  def handle_getheaders(self,peer,payload):
    pass
    
  def handle_tx(self,peer,payload):
    pass
    
  def handle_block(self,peer,payload):
    pass
    
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


