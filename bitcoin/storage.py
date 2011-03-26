import threading
import time
import sqlite3

import bitcoin
import bitcoin.net.payload

blocks_table = """CREATE TABLE blocks(
                    hash BINARY(32) PRIMARY KEY,
                    prev_hash BINARY(32) REFERENCES blocks(hash),
                    merkle_root BINARY(32),
                    timestamp INTEGER,
                    bits INTEGER,
                    nonce BINARY(8),
                    version INTEGER,
                    height INTEGER,
                    )"""

blocks_table = Table('blocks',metadata,
  Column('hash',BINARY(32),unique=True,primary_key=True),
  Column('prev_hash',BINARY(32),ForeignKey('blocks.hash'),nullable=True),
  Column('merkle_root',BINARY(32)),
  Column('timestamp',Integer,index=True),
  Column('bits',Integer,index=True),
  Column('nonce',BINARY(8)),
  Column('version',SmallInteger),
  Column('height',Float,nullable=True,index=True),
)

transactions_table = Table('transactions',metadata,
  Column('hash',BINARY(32),index=True,primary_key=True),
  Column('version',SmallInteger),
  Column('lock_time',Integer,index=True),
  Column('position',Integer,index=True,nullable=True),
  Column('block_hash',BINARY(32),ForeignKey('blocks.hash'),nullable=True),
)

transaction_inputs_table = Table('transaction_inputs',metadata,
  Column('id',Integer,primary_key=True),
  Column('output_hash',BINARY(32),index=True),
  Column('output_index',Integer),
  Column('script',BINARY),
  Column('sequence',Integer),
  Column('position',Integer,index=True),
  Column('transaction_hash',BINARY(32),ForeignKey('transactions.hash')),
)

transaction_outputs_table = Table('transaction_outputs',metadata,
  Column('id',Integer,primary_key=True),
  Column('value',BigInteger,index=True),
  Column('script',BINARY),
  Column('position',Integer,index=True),
  Column('transaction_hash',BINARY(32),ForeignKey('transactions.hash')),
)

metadata.create_all(engine)

mapper(bitcoin.Block,blocks_table,properties={
  'prev_block': relationship(bitcoin.Block,primaryjoin=blocks_table.c.hash==blocks_table.c.prev_hash,remote_side=blocks_table.c.hash,backref=backref('next_blocks')),
  'transactions': relationship(bitcoin.Transaction,order_by=[transactions_table.c.position],collection_class=ordering_list('position'),backref=backref('block')),
})

mapper(bitcoin.Transaction,transactions_table,properties={
  'inputs': relationship(bitcoin.TransactionInput,order_by=[transaction_inputs_table.c.position],collection_class=ordering_list('position')),
  'outputs': relationship(bitcoin.TransactionOutput,order_by=[transaction_outputs_table.c.position],collection_class=ordering_list('position')),
})

mapper(bitcoin.TransactionOutput,transaction_outputs_table)
mapper(bitcoin.TransactionInput,transaction_inputs_table)

genesis_hash = b'o\xe2\x8c\n\xb6\xf1\xb3r\xc1\xa6\xa2F\xaec\xf7O\x93\x1e\x83e\xe1Z\x08\x9ch\xd6\x19\x00\x00\x00\x00\x00'

class Storage(threading.Thread):
  def __init__(self,shutdown,flush_rate=30):
    super(Storage,self).__init__()
    
    self.shutdown = shutdown
    self.flush_rate = flush_rate
    
    self.session = scoped_session(sessionmaker(bind=engine))
    self.session_lock = threading.RLock()
    
    self.block_cache_lock = threading.RLock()
    self.transaction_cache_lock = threading.RLock()
    
    self.transaction_cache = {}
    self.block_cache = {}
    
    self.daemon = True
    
  def get_heads(self):
    print("get_heads")
    with self.transaction_cache_lock and self.block_cache_lock:
      self.flush_caches()
      with self.session_lock:
        print("session_lock")
        return self.session.query(bitcoin.Block).filter(bitcoin.Block.height!=None).filter(not_(bitcoin.Block.hash.in_(self.session.query(bitcoin.Block.prev_hash).filter(bitcoin.Block.height!=None)))).all()
    print("get_heads end")
      
  def get_tails(self):
    print("get_tails")
    with self.transaction_cache_lock and self.block_cache_lock:
      self.flush_caches()
      with self.session_lock:
        print("session_lock")
        return self.session.query(bitcoin.Block).filter(bitcoin.Block.height==None).filter(not_(bitcoin.Block.prev_hash.in_(self.session.query(bitcoin.Block.hash)))).all()
    print("get_tails end")
    
  def run(self):
    while True:
      self.flush_caches()
      if self.shutdown.is_set():
        return
      else:
        time.sleep(self.flush_rate)
      
  def flush_caches(self):
    print("flush_caches")
    self.flush_transaction_cache()
    self.flush_block_cache()
    print("flush_caches end")
  
  def flush_transaction_cache(self):
    print("flush_transaction_cache")
    with self.transaction_cache_lock and self.session_lock:
      print("self.transaction_cache_lock")
      count = 1
      for hash,transaction in self.transaction_cache.items():
        print("session.merge ",count,"/",len(self.transaction_cache))
        self.session.merge(transaction)
        count += 1
      print("transaction commit")
      self.session.commit()
      self.transaction_cache = {}
    print("flush_transaction_cache end")
      
  def flush_block_cache(self):
    print("flush_block_cache")
    with self.block_cache_lock and self.session_lock:
      print("self.block_cache_lock")
      count = 1
      for hash,block in self.block_cache.items():
        print("session.merge ",count,"/",len(self.block_cache))
        self.session.merge(block)
        count += 1
      print("block commit")
      self.session.commit()
      self.block_cache = {}
    print("flush_block_cache end")
  
  def put_transaction(self,transaction):
    with self.transaction_cache_lock:
      self.transaction_cache[transaction.hash] = transaction
      
  def put_block(self,block):
    with self.block_cache_lock:
      if block.hash == genesis_hash:
        block.height = 1.0
      self.block_cache[block.hash] = block
      
  def get_transaction(self,hash):
    with self.transaction_cache_lock:
      if hash in self.transaction_cache:
        return self.transaction_cache[hash]
      else:
        try:
          with self.session_lock:
            return self.session.query(bitcoin.Transaction).filter(bitcoin.Transaction.hash==hash).one()
        except NoResultFound as e:
          return None
        
  def get_block(self,hash):
    with self.block_cache_lock:
      if hash in self.block_cache:
        return self.block_cache[hash]
      else:
        try:
          with self.session_lock:
            return self.session.query(bitcoin.Block).filter(bitcoin.Block.hash==hash).one()
        except NoResultFound as e:
          return None
