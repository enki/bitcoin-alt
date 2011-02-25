#!/usr/bin/env python3
import time
import random
from ProtocolHelper import ProtocolHelper

class Peer:
  version = 32002
  services = 1
  
  def __init__(self,address):
    self.helper = ProtocolHelper(address)
    self.nonce = b''
    for x in range(8):
      self.nonce += bytes([random.randrange(256)])
    self.addr_me = (1,b'\x00'*10+b'\xff'*2+b'\x0A\x2D\x86\x6E',8333)
    self.addr_you = (1,b'\x00'*10+b'\xff'*2+b'\x0A\x2D\x86\x8B',8333)
    
  def poll(self):
    command,payload = self.helper.read_message()
    print(command,payload)
    {'version':self.handle_version,
    'verack':self.handle_verack,
    }[command](payload)
    
  def send_version(self):
    self.helper.send_version(Peer.version,Peer.services,int(time.time()),self.addr_me,self.addr_you,self.nonce,'',110879)
      
  def handle_connect(self,payload):
    pass
  
  def handle_version(self,payload):
    self.helper.send_verack()
    
  def handle_verack(self,payload):
    pass
    
  def handle_addr(self,payload):
    pass
  
  def handle_inv(self,payload):
    pass
  
  def handle_getdata(self,payload):
    pass
  
  def handle_getblocks(self,payload):
    pass
    
  def handle_getheaders(self,payload):
    pass
    
  def handle_tx(self,payload):
    pass
    
  def handle_block(self,payload):
    pass
    
  def handle_headers(self,payload):
    pass
    
  def handle_getaddr(self,payload):
    pass
    
  def handle_checkorder(self,payload):
    pass
    
  def handle_submitorder(self,payload):
    pass
    
  def handle_reply(self,payload):
    pass
    
  def handle_ping(self,payload):
    pass
    
  def handle_alert(self,payload):
    pass
  
if __name__ == "__main__":
  p = Peer(("10.45.134.139",8333))
  p.send_version()
  while True:
    p.poll()
