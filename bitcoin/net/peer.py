import time
import random
import socket
import queue
import threading
import sqlite3

import bitcoin.net.message
import bitcoin.net.payload
import bitcoin.storage

class Peer(threading.Thread):
  def __init__(self,address,peers,shutdown,addr_me={'services': 1, 'addr': '::ffff:127.0.0.1', 'port': 8333},my_version=32002,my_services=1):
    super(Peer,self).__init__()
    
    self.address = address

    self.parser = bitcoin.net.payload.parser()
    
    self.my_nonce = b''
    for x in range(8):
      self.my_nonce += bytes([random.randrange(256)])
    
    self.addr_me = addr_me
    self.addr_you = {'services':1,'addr':address[0],'port':address[1]}
    
    self.my_version = my_version
    self.my_services = my_services
    
    self.peers = peers
    self.slock = threading.Lock()
    self.shutdown = shutdown
    self.daemon = True
    
    self.last_seen = 0
    
    # TODO possibly this should be in run()
    self.socket = socket.socket(socket.AF_INET6)
    self.socket.settimeout(5)
    
    self.reader = bitcoin.net.message.reader(self.socket)
    
  def run(self):
    try:
      self.storage = bitcoin.storage.Storage()# this has to be here so that it's created in the same thread as it's used
    except sqlite3.OperationalError as e:
      pass
    try:
      self.socket.connect(self.address)
      self.socket.settimeout(30)
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
      except KeyboardInterrupt as e:
        return
      finally:
        if self.shutdown.is_set():
          return
        
  def handle_version(self,payload):
    self.version = payload['version']
    self.nonce = payload['nonce']
    self.services = payload['services']
    self.send_verack()
    
  def connect_blocks(self):    
    try:
      self.storage.connect_blocks()
      heads = self.storage.get_heads()
      tails = self.storage.get_tails()
      try:
        self.send_getblocks(heads)
        for tail in tails:
          self.send_getblocks(heads,tail)
      except AttributeError as e:
        pass#this is raised when no version has yet been received
    except sqlite3.OperationalError as e:
      pass
      
  def handle_verack(self,payload):
    self.connect_blocks()
    self.send_getaddr()
    pass
      
  def handle_addr(self,payload):
    for addr in payload['addrs']:
      self.peers.add((addr['node_addr']['addr'],addr['node_addr']['port']))
  
  def handle_inv(self,payload):
    self.connect_blocks()
    invs = []
    for inv in payload['invs']:
      if inv['type'] == 1:
        if not self.storage.get_transaction(inv['hash']):
          invs.append(inv)
      if inv['type'] == 2:
        if not self.storage.get_block(inv['hash']):
          invs.append(inv)
    self.send_getdata(invs)
    
  def handle_getdata(self,payload):
    for inv in payload['invs']:
      if inv['type'] == 1:
        d = self.storage.get_transaction(inv['hash'])
        if d:
          self.send_tx(d)
      if inv['type'] == 2:
        d = self.storage.get_block(inv['hash'])
        if d:
          self.send_block(d)
  
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
    with self.slock:
      try:
        p = bitcoin.net.payload.version(self.my_version,self.my_services,int(time.time()),self.addr_me,self.addr_you,self.my_nonce,'',110879)
        bitcoin.net.message.send(self.socket,b'version',p)
        return True
      except socket.error as e:
        return False
    
  def send_verack(self):
    with self.slock:
      try:
        bitcoin.net.message.send(self.socket,b'verack',b'')
        return True
      except socket.error as e:
        return False
    
  def send_addr(self,addrs):
    with self.slock:
      try:
        p = bitcoin.net.payload.addr(addrs,self.version)
        bitcoin.net.message.send(self.socket,b'addr',p)
        return True
      except socket.error as e:
        return False
      
  def send_inv(self,invs):
    with self.slock:
      try:
        p = bitcoin.net.payload.inv(invs,self.version)
        bitcoin.net.message.send(self.socket,b'inv',p)
        return True
      except socket.error as e:
        return False
    
  def send_getaddr(self):
    with self.slock:
      try:
        bitcoin.net.message.send(self.socket,b'getaddr',b'')
        return True
      except socket.error as e:
        return False
    
  def send_getdata(self,invs):
    with self.slock:
      try:
        p = bitcoin.net.payload.getdata(invs,self.version)
        bitcoin.net.message.send(self.socket,b'getdata',p)
        return True
      except socket.error as e:
        return False
      
  def send_getblocks(self,starts,stop=b'\x00'*32):
    with self.slock:
      try:
        p = bitcoin.net.payload.getblocks(self.version,starts,stop)
        bitcoin.net.message.send(self.socket,b'getblocks',p)
        return True
      except socket.error as e:
        return False
      
  def send_getheaders(self,starts,stop):
    with self.slock:
      try:
        p = bitcoin.net.payload.getheaders(self.version,starts,stop)
        bitcoin.net.message.send(self.socket,b'getheaders',p)
        return True
      except socket.error as e:
        return False
      
  def send_block(self,version,prev_hash,merkle_root,timestamp,bits,nonce,txs):
    with self.slock:
      try:
        p = bitcoin.net.payload.block(version,prev_hash,merkle_root,timestamp,bits,nonce,txs)
        bitcoin.net.message.send(self.socket,b'block',p)
        return True
      except socket.error as e:
        return False
      
  def send_tx(self,tx):
    with self.slock:
      try:
        p = bitcoin.net.payload.tx(tx['version'],tx['tx_ins'],tx['tx_outs'],tx['lock_time'])
        bitcoin.net.message.send(self.socket,b'tx',p)
        return True
      except socket.error as e:
        return False
    
  def send_ping(self):
    with self.slock:
      try:
        bitcoin.net.message.send(self.socket,b'ping',b'')
        return True
      except socket.error as e:
        return False
