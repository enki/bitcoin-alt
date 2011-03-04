import threading
import queue

import bitcoin.net.peer
import bitcoin.storage.peers

class Node:
  static_peers = [("::ffff:10.45.134.110",8333)]
  
  def __init__(self):
    super(Node,self).__init__()
    
    self.open_peers = {}
    self.cb = queue.Queue()
    
    self.shutdown = threading.Event()
    self.shutdown.clear()
    
    for peer in Node.static_peers:
      self.start_peer(peer)
      
  def start_peer(self,peer):
    if not peer in self.open_peers:
      p = bitcoin.net.peer.Peer(peer,self.cb,self.shutdown)
      p.start()
      self.open_peers[peer] = p
    elif not self.open_peers[peer].is_alive():
      self.open_peers[peer].start()
      
    
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
    for addr in payload['addrs']:
      print(addr['node_addr'])
      self.start_peer((addr['node_addr']['addr'],addr['node_addr']['port']))
  
  def handle_inv(self,peer,payload):
    #peer.send_getdata(payload['invs'])
    pass
  
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


