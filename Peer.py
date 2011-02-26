#!/usr/bin/env python3
import time
import random
from ProtocolHelper import ProtocolHelper

class Peer:
  def __init__(self,address):
    self.helper = ProtocolHelper(address)
    self.nonce = b''
    for x in range(8):
      self.nonce += bytes([random.randrange(256)])
    self.addr_me = (1,b'\x00'*10+b'\xff'*2+b'\x0A\x2D\x86\x6E',8333)
    self.addr_you = (1,b'\x00'*10+b'\xff'*2+b'\x0A\x2D\x86\x8B',8333)
    
  def poll(self):
    command,payload = self.helper.read_message()
    
    try:
      {'version':self.handle_version,
      'verack':self.handle_verack,
      'addr':self.handle_addr,
      'inv':self.handle_inv,
      'getdata':self.handle_getdata,
      'getblocks':self.handle_getblocks,
      'getheaders':self.handle_getheaders,
      }[command](payload)
    except KeyError as e:
      print(e,command,payload)
    
  def send_version(self):
    self.helper.send_version(self.helper.my_version,self.helper.services,int(time.time()),self.addr_me,self.addr_you,self.nonce,'',110879)
      
  def handle_connect(self,payload):
    pass
  
  def handle_version(self,payload):
    print("handle_version")
    print(payload)
    self.version = payload['version']
    self.nonce = payload['nonce']
    self.services = payload['services']
    self.helper.send_verack()
    
  def handle_verack(self,payload):
    print("handle_verack")
    print(payload)
    pass
    
  def handle_addr(self,payload):
    print("handle_addr")
    print(payload)
    pass
  
  def handle_inv(self,payload):
    print("handle_inv")
    print(payload)
    self.helper.send_getdata(payload)
    pass
  
  def handle_getdata(self,payload):
    print("handle_getdata")
    print(payload)
    pass
  
  def handle_getblocks(self,payload):
    print("handle_getblocks")
    print(payload)
    
  def handle_getheaders(self,payload):
    print("handle_getheaders")
    print(payload)
    
  def handle_tx(self,payload):
    print("handle_tx")
    print(payload)
    
  def handle_block(self,payload):
    print("handle_block")
    print(payload)
    
  def handle_headers(self,payload):
    print("handle_headers")
    print(payload)
    
  def handle_getaddr(self,payload):
    print("handle_getaddr")
    print(payload)
    
  def handle_checkorder(self,payload):
    print("handle_checkorder")
    print(payload)
    
  def handle_submitorder(self,payload):
    print("handle_submitorder")
    print(payload)
    
  def handle_reply(self,payload):
    print("handle_reply")
    print(payload)
    
  def handle_ping(self,payload):
    print("handle_ping")
    print(payload)
    
  def handle_alert(self,payload):
    print("handle_alert")
    print(payload)
  
if __name__ == "__main__":
  p = Peer(("::ffff:10.45.134.139",8333))
  p.send_version()
  while True:
    p.poll()
