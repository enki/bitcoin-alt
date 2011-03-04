import threading
import queue

import bitcoin.net.peer
import bitcoin.storage.peers

class Node:
  static_nodes = [("::ffff:10.45.134.110",8333)]
  
  def __init__(self):
    super(Node,self).__init__()
    
    self.peers = set()
    self.cb = queue.Queue()
    
    self.shutdown = threading.Event()
    self.shutdown.clear()
    
    for node in Node.static_nodes:
      p = bitcoin.net.peer.Peer(node,self.cb,self.shutdown)
      p.daemon = True
      p.start()
    
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
        continue
      except KeyboardInterrupt as e:
        return
      
  def handle_addr(self,peer,payload):
    pass
  
  def handle_inv(self,peer,payload):
    peer.send_getdata(payload['invs'])
  
  def handle_getdata(self,peer,payload):
    pass
  
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


