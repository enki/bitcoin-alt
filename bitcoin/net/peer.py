import time
import random
import socket
import queue
import threading
import sqlite3

import bitcoin
import bitcoin.net.message
import bitcoin.net.payload
import bitcoin.storage

class Peer(threading.Thread):
  def __init__(self,address,storage,peermanager,shutdown,addr_me=bitcoin.Address('::ffff:127.0.0.1',8333,1),my_version=32002,my_services=1):
    super(Peer,self).__init__()
    
    self.address = address
    self.storage = storage
    self.peermanager = peermanager
    self.shutdown = shutdown
    self.addr_me = addr_me
    self.addr_you = bitcoin.Address(address[0],address[1],1)
    self.my_version = my_version
    self.my_services = my_services
    
    self.my_nonce = b''
    for x in range(8):
      self.my_nonce += bytes([random.randrange(256)])
      
    self.version = None
    
    self.socket_lock = threading.Lock()
    self.daemon = True
    
    self.last_seen = 0
    
    self.parser = bitcoin.net.payload.parser()
    
  def run(self):
    try:
      self.socket = socket.socket(socket.AF_INET6)
      self.socket.settimeout(5) # timeout faster for connect()      
      self.socket.connect(self.address)
      self.socket.settimeout(30)
      self.reader = bitcoin.net.message.reader(self.socket)
      self.send_version()
    except socket.timeout as e:
      return
    except socket.error as e:
      if e.errno == 111:
        return
      elif e.errno == 113:
        return
      else:
        raise e
        
    while True:
      try:
        command,raw_payload = self.reader.read()
      except socket.timeout as e:
        self.send_ping()
        continue
      except socket.error as e:
        return
      finally:
        if self.shutdown.is_set():
          return
          
      self.last_seen = time.time()

      payload = self.parser.parse(command,raw_payload)
      
      if command != 'version' and command != 'verack' and not self.version:
        raise Exception("received packet before version")
             
      try: 
        {'version':self.handle_version,
        'verack':self.handle_verack,
        'ping':self.handle_ping,
        'addr':self.handle_addr,
        'inv':self.handle_inv,
        'getdata':self.handle_getdata,
        'getblocks':self.handle_getblocks,
        'getheaders':self.handle_getheaders,
        'tx':self.handle_tx,
        'block':self.handle_block,
        'getaddr':self.handle_getaddr,
        'alert':self.handle_alert,
        }[command](payload)
      except queue.Empty as e:
        pass
      finally:
        if self.shutdown.is_set():
          return
        
  def handle_version(self,payload):
    self.version = payload['version']
    self.nonce = payload['nonce']
    self.services = payload['services']
    self.send_verack()
      
  def handle_verack(self,payload):
    self.send_getaddr()
      
  def handle_addr(self,payload):
    for addr in payload:
      self.peermanager.add((addr.addr,addr.port))
  
  def handle_inv(self,payload):
    invs = []
    for inv in payload:
      if inv['type'] == 1:
        transaction = self.storage.get_transaction(inv['hash'])
        if not transaction:
          invs.append(inv)
      if inv['type'] == 2:
        block = self.storage.get_block(inv['hash'])
        if not block:
          invs.append(inv)

    if invs:
      self.send_getdata(invs)
    
  def handle_getdata(self,payload):
    invs = []
    for inv in payload:
      if inv['type'] == 1:
        transaction = self.storage.get_transaction(inv['hash'])
        if not transaction:
          self.send_transaction(transaction)
      if inv['type'] == 2:
        block = self.storage.get_block(inv['hash'])
        if not block:
          self.send_block(block)
  
  def handle_getblocks(self,payload):
    pass
    
  def handle_getheaders(self,payload):
    pass
    
  def handle_tx(self,transaction):
    self.storage.put_transaction(transaction)
    
  def handle_block(self,block):
    self.storage.put_block(block)
    
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
    
  def handle_alert(self,payload):
    pass

  def handle_ping(self,payload):
    pass
    
  def send_version(self):
    with self.socket_lock:
      try:
        p = bitcoin.net.payload.version(self.my_version,self.my_services,int(time.time()),self.addr_me,self.addr_you,self.my_nonce,'',110879)#TODO fixed value
        bitcoin.net.message.send(self.socket,b'version',p)
        return True
      except socket.error as e:
        return False
    
  def send_verack(self):
    with self.socket_lock:
      try:
        bitcoin.net.message.send(self.socket,b'verack',b'')
        return True
      except socket.error as e:
        return False
    
  def send_addr(self,addrs):
    if not self.version:
      return False
    with self.socket_lock:
      try:
        p = bitcoin.net.payload.addr(addrs,self.version)
        bitcoin.net.message.send(self.socket,b'addr',p)
        return True
      except socket.error as e:
        return False
      
  def send_inv(self,invs):
    if not self.version:
      return False
    with self.socket_lock:
      try:
        p = bitcoin.net.payload.inv(invs,self.version)
        bitcoin.net.message.send(self.socket,b'inv',p)
        return True
      except socket.error as e:
        return False
    
  def send_getaddr(self):
    with self.socket_lock:
      try:
        bitcoin.net.message.send(self.socket,b'getaddr',b'')
        return True
      except socket.error as e:
        return False
    
  def send_getdata(self,invs):
    if not self.version:
      return False
    with self.socket_lock:
      try:
        p = bitcoin.net.payload.getdata(invs,self.version)
        bitcoin.net.message.send(self.socket,b'getdata',p)
        return True
      except socket.error as e:
        return False
      
  def send_getblocks(self,starts,stop=b'\x00'*32):
    if not self.version:
      return False
    with self.socket_lock:
      try:
        p = bitcoin.net.payload.getblocks(self.version,starts,stop)
        bitcoin.net.message.send(self.socket,b'getblocks',p)
        return True
      except socket.error as e:
        return False
      
  def send_getheaders(self,starts,stop):
    if not self.version:
      return False
    with self.socket_lock:
      try:
        p = bitcoin.net.payload.getheaders(self.version,starts,stop)
        bitcoin.net.message.send(self.socket,b'getheaders',p)
        return True
      except socket.error as e:
        return False
      
  def send_block(self,block):
    with self.socket_lock:
      try:
        p = bitcoin.net.payload.block(block)
        bitcoin.net.message.send(self.socket,b'block',p)
        return True
      except socket.error as e:
        return False
      
  def send_transaction(self,transaction):
    with self.socket_lock:
      try:
        p = bitcoin.net.payload.transaction(transaction)
        bitcoin.net.message.send(self.socket,b'tx',p)
        return True
      except socket.error as e:
        return False
    
  def send_ping(self):
    with self.socket_lock:
      try:
        bitcoin.net.message.send(self.socket,b'ping',b'')
        return True
      except socket.error as e:
        return False
