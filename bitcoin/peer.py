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
  def __init__(self,address,storage,peers,shutdown,addr_me=bitcoin.Address('::ffff:127.0.0.1',8333,1),my_version=32002,my_services=1):
    super(Peer,self).__init__()
    
    self.address = address
    self.storage = storage

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
    
    self.session = bitcoin.storage.session
    
  def run(self):
    try:
      # TODO possibly this should be in run()
      self.socket = socket.socket(socket.AF_INET6)
      self.socket.settimeout(5)
      self.socket.connect(self.address)
      self.socket.settimeout(30)
      self.reader = bitcoin.net.message.reader(self.socket)
      self.send_version()
    except socket.timeout:
      return
    except socket.error as e:
      if e.errno == 111:
        return
      elif e.errno == 113:
        return
      else:
        raise e
        
    replay = None
    while True:
      try:
        if replay:
          command,payload = replay
          replay = None
        else:
          command,payload = self.read_message()
        
        self.last_seen = time.time()
        
        if command != 'version' and command != 'verack' and not self.version:
          raise Exception("received packet before version")
        
        if command == 'version':
          self.version = payload['version']
          self.nonce = payload['nonce']
          self.services = payload['services']
          self.send_verack()
        elif command == 'verack':
          genesis_block = self.storage.get_block(bitcoin.storage.genesis_hash)
          if genesis_block:
            print("requesting genesis block")
            self.send_getdata([{'type':2,'hash':bitcoin.storage.genesis_hash}])
            self.send_getblocks([bitcoin.storage.genesis_hash])
          else:
            self.connect_blocks()
          
          self.send_getaddr()

        elif command == 'addr':
          print("addr")
          for addr in payload:
            self.peers.add((addr.addr,addr.port))
        elif command == 'inv':
          print("inv",len(payload))
          start = time.time()
          block_hashs = [inv['hash'] for inv in payload if inv['type'] == 2]
          transaction_hashs = [inv['hash'] for inv in payload if inv['type'] == 1]
          invs = []
          
          if block_hashs:
            # TODO this should include the transactions associated with the blocks
            blocks = self.storage.get_blocks(block_hashs)
            for block in blocks:
              block_hashs.remove(block.hash)
            if block_hashs:
              invs.extend([{'type':2,'hash':block_hash} for block_hash in block_hashs])
          
          if transaction_hashs:
            # TODO do we already have these transactions?
            transactions = self.storage.get_transactions(transaction_hashs)
            for transaction in transactions:
              transaction_hashs.remove(transaction.hash)
            if transaction_hashs:
              invs.extend([{'type':1,'hash':transaction_hash} for transaction_hash in transaction_hashs])
          
          if invs:
            self.send_getdata(invs)
          print("inv end",time.time()-start)
          self.connect_blocks()
        elif command == 'tx':
          print("tx")
          transactions = [payload]
          command,payload = self.read_message()
          while command == 'tx':
            transactions.append(payload)
            command,payload = self.read_message()
          try:
            # TODO merge the transactions with whatever information we already have about the transaction everything is static except the block_hash which is write once
            for transaction in transactions:
              self.session.merge(transaction)
          except IntegrityError:
            self.session.rollback()
            for transaction in transactions:
              self.session.merge(transaction)
          self.session.commit()
          replay = (command,payload)
        elif command == 'block':
          print("block")
          start = time.time()
          blocks = [payload]
          command,payload = self.read_message()
          while command == 'block':
            blocks.append(payload)
            command,payload = self.read_message()
          try:
            s1 = time.time()
            for block in blocks:
              if block.hash == bitcoin.storage.genesis_hash:
                block.height = 1.0
                
              # TODO merge the blocks with whatever information we already have about the block, everything is static except the height of the block which is write once
              s=time.time()
              print("session.merge(block)")
              if type(block.hash) is not bytes:
                print("type(block.hash)",block.hash)
              if type(block.prev_hash) is not bytes:
                print("type(block.prev_hash)",block.prev_hash)
              self.session.merge(block)
              print("session.merge(block) end",time.time()-s)
            print("merged ",time.time()-s1)
          except IntegrityError:# TODO this is kind of a race condition, overlapping block inserts could result in multiple IntegrityErrors
            self.session.rollback()
            for block in blocks:
              if block.hash == bitcoin.storage.genesis_hash:
                block.height = 1.0
              self.session.merge(block)
          s=time.time()
          self.session.commit()
          print("session.commit()",time.time()-s)
          replay = (command,payload)
          print("block end",time.time()-start)
        elif command == 'getdata':
          print("getdata")
          block_hashs = [inv['hash'] for inv in payload if inv['type'] == 2]
          transaction_hashs = [inv['hash'] for inv in payload if inv['type'] == 1]
          invs = []
          if block_hashs:
            # TODO do we have these blocks?
            blocks = self.session.query(bitcoin.Block).filter(bitcoin.Block.hash.in_(block_hashs)).all()
            if blocks:
              for block in blocks:
                self.send_block(block)
          
          if transaction_hashs:
            # TODO do we already have these transactions?
            transactions = self.session.query(bitcoin.Transaction).filter(bitcoin.Transaction.hash.in_(transaction_hashs)).all()
            if transactions:
              for transaction in transactions:
                self.send_transaction(transaction)
                
      except socket.error:
        self.session.commit()
        return
      finally:
        if self.shutdown.is_set():
          self.session.commit()
          return
          
  def read_message(self):
    try:
      command,raw_payload = self.reader.read()
      payload = self.parser.parse(command,raw_payload)
    except socket.timeout:
      self.send_ping()
      return self.read_message()
    return (command,payload)

  def get_heads(self):
    # TODO find the heads of the verified block chain (blocks with a height)
    return self.session.query(bitcoin.Block).filter(bitcoin.Block.height!=None).filter(not_(bitcoin.Block.hash.in_(self.session.query(bitcoin.Block.prev_hash).filter(bitcoin.Block.height!=None)))).all()
      
  def get_tails(self):
    # TODO find the tails of the unverified blocks (blocks without a height)
    return self.session.query(bitcoin.Block).filter(bitcoin.Block.height==None).filter(not_(bitcoin.Block.prev_hash.in_(self.session.query(bitcoin.Block.hash)))).all()
      
  def connect_blocks(self):
    print("connect_blocks")
    # TODO walk the block chain setting the height as we go
    start = time.time()
    try:
      heads = self.get_heads()
      while heads:
        head = heads.pop()
        if head.next_blocks:
          for next_block in head.next_blocks:
            next_block.height = head.height + next_block.difficulty()
            self.session.merge(next_block)
            heads.append(next_block)
      
      self.session.commit()
      heads = self.get_heads()
      if heads:
        self.send_getblocks([block.hash for block in heads])
    except OperationalError as e:
      print("OperationalError")
      self.session.rollback()
      
    end = time.time()
    print("connect_blocks end",end-start)
  
  def send_version(self):
    print("send_version")
    with self.socket_lock:
      try:
        p = bitcoin.net.payload.version(self.my_version,self.my_services,int(time.time()),self.addr_me,self.addr_you,self.my_nonce,'',110879)
        bitcoin.net.message.send(self.socket,b'version',p)
        return True
      except socket.error as e:
        return False
    
  def send_verack(self):
    print("send_verack")
    with self.socket_lock:
      try:
        bitcoin.net.message.send(self.socket,b'verack',b'')
        return True
      except socket.error as e:
        return False
    
  def send_addr(self,addrs):
    print("send_addr")
    with self.socket_lock:
      try:
        p = bitcoin.net.payload.addr(addrs,self.version)
        bitcoin.net.message.send(self.socket,b'addr',p)
        return True
      except socket.error as e:
        return False
      
  def send_inv(self,invs):
    print("send_inv")
    with self.socket_lock:
      try:
        p = bitcoin.net.payload.inv(invs,self.version)
        bitcoin.net.message.send(self.socket,b'inv',p)
        return True
      except socket.error as e:
        return False
    
  def send_getaddr(self):
    print("send_getaddr")
    with self.socket_lock:
      try:
        bitcoin.net.message.send(self.socket,b'getaddr',b'')
        return True
      except socket.error as e:
        return False
    
  def send_getdata(self,invs):
    print("send_getdata")
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
    print("send_getheaders")
    with self.socket_lock:
      try:
        p = bitcoin.net.payload.getheaders(self.version,starts,stop)
        bitcoin.net.message.send(self.socket,b'getheaders',p)
        return True
      except socket.error as e:
        return False
      
  def send_block(self,version,prev_hash,merkle_root,timestamp,bits,nonce,txs):
    print("send_block")
    with self.socket_lock:
      try:
        p = bitcoin.net.payload.block(version,prev_hash,merkle_root,timestamp,bits,nonce,txs)
        bitcoin.net.message.send(self.socket,b'block',p)
        return True
      except socket.error as e:
        return False
      
  def send_transaction(self,tx):
    print("send_tx")
    with self.socket_lock:
      try:
        p = bitcoin.net.payload.tx(tx['version'],tx['tx_ins'],tx['tx_outs'],tx['lock_time'])
        bitcoin.net.message.send(self.socket,b'tx',p)
        return True
      except socket.error as e:
        return False
    
  def send_ping(self):
    print("send_ping")
    with self.socket_lock:
      try:
        bitcoin.net.message.send(self.socket,b'ping',b'')
        return True
      except socket.error as e:
        return False
