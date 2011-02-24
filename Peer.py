#!/usr/bin/env python3
from ProtocolHelper import ProtocolHelper

class Peer:
  def __init__(self,address):
    self.helper = ProtocolHelper(address)
    
  def poll(self):
    command,payload = self.helper.read_message_header()
    print(command,payload)
      
  def handle_connect(self):
    pass
  
  def handle_version(self):
    pass
    
  def handle_verack(self):
    pass
    
  def handle_addr(self):
    pass
  
  def handle_inv(self):
    pass
  
  def handle_getdata(self):
    pass
  
  def handle_getblocks(self):
    pass
    
  def handle_getheaders(self):
    pass
    
  def handle_tx(self):
    pass
    
  def handle_block(self):
    pass
    
  def handle_headers(self):
    pass
    
  def handle_getaddr(self):
    pass
    
  def handle_checkorder(self):
    pass
    
  def handle_submitorder(self):
    pass
    
  def handle_reply(self):
    pass
    
  def handle_ping(self):
    pass
    
  def handle_alert(self):
    pass
    
if __name__ == "__main__":
  p = Peer(("10.45.134.139",8333))
  p.poll()
