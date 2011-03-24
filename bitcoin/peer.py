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

from sqlalchemy.sql.expression import not_
from sqlalchemy.orm import mapper,relationship, scoped_session, sessionmaker
from sqlalchemy.orm.exc import NoResultFound

class Peer(threading.Thread):
  def __init__(self,address,peers,shutdown,addr_me=bitcoin.Address('::ffff:127.0.0.1',8333,1),my_version=32002,my_services=1):
    super(Peer,self).__init__()
    
    self.address = address

    self.parser = bitcoin.net.payload.parser()
    
    self.my_nonce = b''
    for x in range(8):
      self.my_nonce += bytes([random.randrange(256)])
    
    self.addr_me = addr_me
    self.addr_you = bitcoin.Address(address[0],address[1],1)
    
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
    
    self.genesis_block = None
    
    self.reader = bitcoin.net.message.reader(self.socket)
    
    self.session = scoped_session(sessionmaker(bind=bitcoin.storage.engine))
    self.session.execute("PRAGMA synchronous=OFF;")
    
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
          
  def difficulty(self,bits):
    target = (bits & 0x00ffffff)*(2**(8*((bits >> 24) - 3))) 
    max_target = 0x00000000ffff0000000000000000000000000000000000000000000000000000
    return max_target/target

  def get_heads(self):
    return self.session.query(bitcoin.Block.hash).filter(bitcoin.Block.height!=None).filter(not_(bitcoin.Block.hash.in_(self.session.query(bitcoin.Block.prev_hash)))).all()
      
  def get_tails(self):
    return self.session.query(bitcoin.Block.hash).filter(bitcoin.Block.height!=None).filter(not_(bitcoin.Block.prev_hash.in_(self.session.query(bitcoin.Block.hash)))).all()
      
  def connect_blocks(self):
    heads = set(self.get_heads())
    for head in heads:
      self.connect_head(head)
    self.session.commit()
    heads = self.get_heads()
    try:
      self.send_getblocks(heads)
    except AttributeError as e:
      pass#this is raised when no version has yet been received
  
  def connect_head(self,head):
    for next_block in head.next_blocks:
      next_block.height = head.height + difficulty(next_block.bits)
      self.connect_head(next_block)
        
  def handle_version(self,payload):
    self.version = payload['version']
    self.nonce = payload['nonce']
    self.services = payload['services']
    self.send_verack()
      
  def handle_verack(self,payload):
    try:
      genesis_block = self.session.query(bitcoin.Block).filter_by(hash=bitcoin.storage.genesis_hash).one()
    except NoResultFound as e:
      self.send_getblocks([bitcoin.storage.genesis_hash])
    
    self.connect_blocks()
    self.send_getaddr()
      
  def handle_addr(self,payload):
    for addr in payload:
      self.peers.add((addr.addr,addr.port))
  
  def handle_inv(self,payload):
    self.connect_blocks()
    invs = []
    for inv in payload:
      if inv['type'] == 1:
        try:
          self.session.query(bitcoin.Transaction).filter_by(hash=inv['hash']).one()
        except NoResultFound as e:
          invs.append(inv)
      if inv['type'] == 2:
        try:
          self.session.query(bitcoin.Block).filter_by(hash=inv['hash']).one()
        except NoResultFound as e:
          invs.append(inv)
    if invs:
      self.send_getdata(invs)
    
  def handle_getdata(self,payload):
    for inv in payload:
      if inv['type'] == 1:
        try:
          transaction = self.session.query(bitcoin.Transaction).filter_by(hash=inv['hash']).one()
        except NoResultFound as e:
          pass
        else:
          self.send_tx(transaction)
      if inv['type'] == 2:
        try:
          block = self.session.query(bitcoin.Block).filter_by(hash=inv['hash']).one()
        except NoResultFound as e:
          pass
        else:
          self.send_block(block)
  
  def handle_getblocks(self,payload):
    pass
    
  def handle_getheaders(self,payload):
    pass
    
  def handle_tx(self,transaction):
    self.session.add(transaction)
    self.session.commit()
    
  def handle_block(self,block):
    if block.hash == bitcoin.storage.genesis_hash:
      block.height = 1.0
    self.session.add(block)
    self.session.commit()
    
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
