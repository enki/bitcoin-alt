import time
import random
import socket
import queue
import threading

import bitcoin.net.message
import bitcoin.net.payload

class Peer(threading.Thread):
  def __init__(self,address,cb,shutdown,addr_me={'services': 1, 'addr': '::ffff:127.0.0.1', 'port': 8333},my_version=32002,my_services=1):
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
    
    self.cb = cb
    self.slock = threading.Lock()
    self.shutdown = shutdown
    self.daemon = True
    
    self.last_seen = 0
    
    # possibly this should be in run()
    self.socket = socket.socket(socket.AF_INET6)
    self.socket.settimeout(5)
    
    self.reader = bitcoin.net.message.reader(self.socket)
    
  def run(self):
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

      p = self.parser.parse(command,raw_payload)
      
      if command == 'version':
        self.version = p['version']
        self.nonce = p['nonce']
        self.services = p['services']
        self.send_verack()
      elif command == 'verack':
        self.cb.put({'peer':self,'command':'connect','payload':{}})
      elif command == 'ping':
        pass
      elif self.version:
        self.cb.put({'peer':self,'command':command,'payload':p})
      else:
        raise Exception("received packet before version")
    
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
      
  def send_getblocks(self,starts,stop):
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
