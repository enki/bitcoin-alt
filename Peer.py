#!/usr/bin/env python3
import time
import random
import socket

import bitcoin.net.message
import bitcoin.net.payload

class Peer:
  my_version = 32002
  my_services = 1

  def __init__(self,address,addr_me=(1,'::ffff:127.0.0.1',8333)):
  
    self.address = address
    self.socket = socket.socket(socket.AF_INET6)
    self.socket.connect(address)
    
    self.reader = bitcoin.net.message.reader(self.socket)
    self.parser = bitcoin.net.payload.parser()
    
    self.my_nonce = b''
    for x in range(8):
      self.my_nonce += bytes([random.randrange(256)])
    
    self.addr_me = addr_me
    self.addr_you = (1,address[0],address[1])
    
  def poll(self):
    command,raw_payload = self.reader.read()
    p = self.parser.parse(command,raw_payload)
    
    if p:
      try:
        {'version':self.handle_version,
        'verack':self.handle_verack,
        'addr':self.handle_addr,
        'inv':self.handle_inv,
        'getdata':self.handle_getdata,
        'getblocks':self.handle_getblocks,
        'getheaders':self.handle_getheaders,
        'tx':self.handle_tx,
        'block':self.handle_block,
        'getaddr':self.handle_getaddr,
        'ping':self.handle_ping,
        'alert':self.handle_alert,
        }[command](p)
      except KeyError as e:
        print(e,command,p)
    
  def send_version(self):
    p = bitcoin.net.payload.version(self.my_version,self.my_services,int(time.time()),self.addr_me,self.addr_you,self.my_nonce,'',110879)
    bitcoin.net.message.send(self.socket,b'version',p)
    
  def send_verack(self):
    bitcoin.net.message.send(self.socket,b'verack',b'')
    
  def send_inv(self,invs):
    p = bitcoin.net.payload.inv(invs,self.version)
    bitcoin.net.message.send(self.socket,b'inv',p)
    
  def send_getdata(self,invs):
    p = bitcoin.net.payload.getdata(invs,self.version)
    bitcoin.net.message.send(self.socket,b'getdata',p)
  
  def handle_version(self,p):
    print("handle_version")
    print(p)
    self.version = p['version']
    self.nonce = p['nonce']
    self.services = p['services']
    self.send_verack()
    
  def handle_verack(self,p):
    print("handle_verack")
    print(p)
    
  def handle_addr(self,p):
    print("handle_addr")
    print(p)
  
  def handle_inv(self,p):
    print("handle_inv")
    print(p)
    self.send_getdata(p['invs'])
  
  def handle_getdata(self,p):
    print("handle_getdata")
    print(p)
  
  def handle_getblocks(self,p):
    print("handle_getblocks")
    print(p)
    
  def handle_getheaders(self,p):
    print("handle_getheaders")
    print(p)
    
  def handle_tx(self,p):
    print("handle_tx")
    print(p)
    
  def handle_block(self,p):
    print("handle_block")
    print(p)
    
  def handle_headers(self,p):
    print("handle_headers")
    print(p)
    
  def handle_getaddr(self,p):
    print("handle_getaddr")
    print(p)
    
  def handle_checkorder(self,p):
    print("handle_checkorder")
    print(p)
    
  def handle_submitorder(self,p):
    print("handle_submitorder")
    print(p)
    
  def handle_reply(self,p):
    print("handle_reply")
    print(p)
    
  def handle_ping(self,p):
    print("handle_ping")
    print(p)
    
  def handle_alert(self,p):
    print("handle_alert")
    print(p)
  
if __name__ == "__main__":
  #p = Peer(("::ffff:10.45.134.139",8333))
  p = Peer(("::ffff:10.45.134.110",8333))
  p.send_version()
  while True:
    p.poll()
