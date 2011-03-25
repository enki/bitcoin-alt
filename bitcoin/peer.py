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
from sqlalchemy.exc import IntegrityError,OperationalError

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
    self.socket_lock = threading.Lock()
    self.shutdown = shutdown
    self.daemon = True
    
    self.last_seen = 0
    
    # TODO possibly this should be in run()
    self.socket = socket.socket(socket.AF_INET6)
    self.socket.settimeout(5)
    
    self.reader = bitcoin.net.message.reader(self.socket)
    
    self.session = bitcoin.storage.session
    
    self.requested_heads = set()
    self.requested_blocks = set()
    self.requested_transactions = set()
    
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
        self.session.commit()
        return
      finally:
        if self.shutdown.is_set():
          self.session.commit()
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
          self.session.commit()
          return

  def get_heads(self):
    return self.session.query(bitcoin.Block).filter(bitcoin.Block.height!=None).filter(not_(bitcoin.Block.hash.in_(self.session.query(bitcoin.Block.prev_hash).filter(bitcoin.Block.height!=None)))).all()
      
  def get_tails(self):
    return self.session.query(bitcoin.Block).filter(bitcoin.Block.height==None).filter(not_(bitcoin.Block.prev_hash.in_(self.session.query(bitcoin.Block.hash)))).all()
      
  def connect_blocks(self):
    try:
      heads = self.get_heads()
      while heads:
        head = heads.pop()
        if head.next_blocks:
          for next_block in head.next_blocks:
            next_block.height = head.height + next_block.difficulty()
            self.session.add(next_block)
            heads.append(next_block)
            
      heads = set((head.hash for head in self.get_heads())).difference(self.requested_heads)
      if heads:
        self.requested_heads.update(heads)
        self.send_getblocks(heads)
    except OperationalError as e:
      self.session.rollback()
        
  def handle_version(self,payload):
    self.version = payload['version']
    self.nonce = payload['nonce']
    self.services = payload['services']
    self.send_verack()
      
  def handle_verack(self,payload):
    try:
      genesis_block = self.session.query(bitcoin.Block).filter_by(hash=bitcoin.storage.genesis_hash).one()
    except NoResultFound as e:
      self.send_getdata([{'type':2,'hash':bitcoin.storage.genesis_hash}])
      self.send_getblocks([bitcoin.storage.genesis_hash])
    
    self.send_getaddr()
    self.connect_blocks()
      
  def handle_addr(self,payload):
    for addr in payload:
      self.peers.add((addr.addr,addr.port))
  
  def handle_inv(self,payload):
    self.session.commit()
    invs = []
    for inv in payload:
      if inv['type'] == 1:
        try:
          self.session.query(bitcoin.Transaction).filter(bitcoin.Transaction.hash==inv['hash']).one()
        except NoResultFound as e:
          if inv['hash'] not in self.requested_transactions:
            self.requested_transactions.add(inv['hash'])
            invs.append(inv)
      if inv['type'] == 2:
        try:
          self.session.query(bitcoin.Block).filter(bitcoin.Block.hash==inv['hash']).one()
        except NoResultFound as e:
          if inv['hash'] not in self.requested_blocks:
            self.requested_blocks.add(inv['hash'])
            invs.append(inv)

    if invs:
      self.send_getdata(invs)
    
    self.connect_blocks()
    
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
    try:
      with bitcoin.storage.flush_lock:
        transaction = self.session.merge(transaction)
        self.session.add(transaction)
        self.session.flush()
    except IntegrityError as e:
      print("Rollback transaction ",block.transaction)
      self.session.rollback()
    except OperationalError as e:
      self.session.rollback()
    finally:
      try:
        self.requested_transactions.remove(transaction.hash)
      except KeyError as e:
        pass
    
  def handle_block(self,block):
    try:
      if block.hash == bitcoin.storage.genesis_hash:
        block.height = 1.0
      with bitcoin.storage.flush_lock:
        block = self.session.merge(block)
        self.session.add(block)
        self.session.flush()
    except IntegrityError as e:
      print("Rollback block ",block.hash)
      self.session.rollback()
    except OperationalError as e:
      self.session.rollback()
    finally:
      try:
        self.requested_blocks.remove(block.hash)
      except KeyError as e:
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
    
  def handle_alert(self,payload):
    pass

  def handle_ping(self,payload):
    pass
    
  def send_version(self):
    with self.socket_lock:
      try:
        p = bitcoin.net.payload.version(self.my_version,self.my_services,int(time.time()),self.addr_me,self.addr_you,self.my_nonce,'',110879)
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
    with self.socket_lock:
      try:
        p = bitcoin.net.payload.addr(addrs,self.version)
        bitcoin.net.message.send(self.socket,b'addr',p)
        return True
      except socket.error as e:
        return False
      
  def send_inv(self,invs):
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
    with self.socket_lock:
      try:
        p = bitcoin.net.payload.getdata(invs,self.version)
        bitcoin.net.message.send(self.socket,b'getdata',p)
        return True
      except socket.error as e:
        return False
      
  def send_getblocks(self,starts,stop=b'\x00'*32):
    print("send_getblocks",starts,stop)
    with self.socket_lock:
      try:
        p = bitcoin.net.payload.getblocks(self.version,starts,stop)
        bitcoin.net.message.send(self.socket,b'getblocks',p)
        return True
      except socket.error as e:
        return False
      
  def send_getheaders(self,starts,stop):
    with self.socket_lock:
      try:
        p = bitcoin.net.payload.getheaders(self.version,starts,stop)
        bitcoin.net.message.send(self.socket,b'getheaders',p)
        return True
      except socket.error as e:
        return False
      
  def send_block(self,version,prev_hash,merkle_root,timestamp,bits,nonce,txs):
    with self.socket_lock:
      try:
        p = bitcoin.net.payload.block(version,prev_hash,merkle_root,timestamp,bits,nonce,txs)
        bitcoin.net.message.send(self.socket,b'block',p)
        return True
      except socket.error as e:
        return False
      
  def send_tx(self,tx):
    with self.socket_lock:
      try:
        p = bitcoin.net.payload.tx(tx['version'],tx['tx_ins'],tx['tx_outs'],tx['lock_time'])
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
