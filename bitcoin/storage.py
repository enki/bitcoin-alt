import threading

from sqlalchemy import create_engine,Table,Column,MetaData,ForeignKey,DateTime,Integer,BigInteger,SmallInteger,Float
from sqlalchemy.types import BINARY
from sqlalchemy.orm import mapper,relation, scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.orderinglist import ordering_list
from sqlalchemy.sql.expression import not_
from sqlalchemy.orm.exc import NoResultFound


engine = create_engine('sqlite:///bitcoin.sqlite3', echo=True)
session = scoped_session(sessionmaker(bind=engine))
Base = declarative_base()

class Block(Base):
  __tablename__ = 'block'
  blockid = Column(Integer, primary_key=True)
  blockhash = Column(BINARY(32),unique=True)
  previoushash = Column(BINARY(32),ForeignKey('block.blockhash'),unique=True)
  merkle_root = Column(BINARY(32))
  timestamp = Column(DateTime,index=True)
  bits = Column(Integer,index=True)
  nonce = Column(BINARY(8))
  version = Column(SmallInteger)
  height = Column(Float,nullable=True,index=True)

  previousblock = relation('Block', uselist=False)
  
class Transaction(Base):
  __tablename__ = 'transaction'
  transactionid = Column(Integer,primary_key=True)
  blockhash = Column(BINARY(32),index=True)
  sequence = Column(Integer,index=True)
  version = Column(SmallInteger)
  locktime = Column(DateTime,index=True)
  blockid = Column(Integer,ForeignKey('block.blockid'),nullable=True)

  block = relation('Block', backref='transactions')

class TransactionIn(Base):
  __tablename__ = 'transactionin'
  transinid = Column(Integer,primary_key=True)
  outhash = Column(BINARY(32),index=True)
  outindex = Column(Integer)
  script = Column(BINARY)
  sequence = Column(Integer)
  transactionid = Column(Integer,ForeignKey('transaction.transactionid'))

  transaction = relation('Transaction', backref='transactionsin')

class TransactionOut(Base):
  __tablename__ = 'transactionout'
  transoutid = Column(Integer,primary_key=True)
  value = Column(BigInteger,index=True)
  script = Column(BINARY)
  sequence = Column(Integer)
  transactionid = Column(Integer,ForeignKey('transaction.transactionid'))

  transaction = relation('Transaction', backref='transactionsout')

Base.metadata.create_all(engine)

class Storage(object):
  genesis_hash = b'o\xe2\x8c\n\xb6\xf1\xb3r\xc1\xa6\xa2F\xaec\xf7O\x93\x1e\x83e\xe1Z\x08\x9ch\xd6\x19\x00\x00\x00\x00\x00'    
  
  def __init__(self):
    
    self.dlock = threading.RLock()
    self.tx_cache = {}
    self.block_cache = {}
    
    #self.put_block(Storage.genesis_block)

  def difficulty(self,bits):
    target = (bits & 0x00ffffff)*(2**(8*((bits >> 24) - 3))) 
    max_target = 0x00000000ffff0000000000000000000000000000000000000000000000000000
    return max_target/target
    
  def get_heads(self):
    with self.dlock:
      self.flush_tx_cache()
      self.flush_block_cache()

      c = session.query(Block).filter(Block.height!=None).filter(not_(Block.blockhash.in_(session.query(Block.previoushash))))
      return [h.blockhash for h in c]
      
  def get_tails(self):
    with self.dlock:
      self.flush_tx_cache()
      self.flush_block_cache()

      c = session.query(Block).filter(Block.height!=None).filter(not_(Block.previoushash.in_(session.query(Block.blockhash))))
      return [h.blockhash for h in c]
      
  def connect_blocks(self):
    with self.dlock:
      nullheightblocks = session.query(Block.blockhash).filter(Block.height!=None)
      c = session.query(Block).filter(Block.height!=None).filter(Block.previoushash.in_(nullheightblocks))
      for start_block in c:
        block = start_block
        while block:
          prev_block = block.previousblock
          block.height = self.difficulty(block.bits) + prev_block.height
          block = prev_block
      session.commit()
  
  def get_tx(self,h):
    with self.dlock:
      try:
        transaction = session.query(Transaction).filter_by(blockhash=h).one()
      except NoResultFound:
        return None

      return {
        'hash': h,
        'version': transaction.version,
        'lock_time': transaction.locktime,
        'block_hash': transaction.blockhash,
        'tx_ins': [{
            'outpoint': {'out_index': inbound.outindex,
                         'out_hash': inbound.outhash},
            'script': r.script,
            'sequence': r.sequence} for inbound in transaction.transactionsin],
        'tx_outs': [{
            'value': outbound.value,
            'pk_script': outbound.script} for outbound in transaction.transactionsout]
        }
        
  def get_block(self,h):
    with self.dlock:
      try:
        return session.query(Block).filter_by(blockhash=h)
      except NoResultFound:
        return None
        
  def get_next_block(self,h):
    with self.dlock:
      try:
        return session.query(Block).filter_by(previoushash=h)
      except NoResultFound:
        return None
    
  def put_tx(self,tx,sequence=None,block=None):
    with self.dlock:
      tx['block'] = block
      tx['sequence'] = sequence
      
      if tx['hash'] not in self.tx_cache:
        self.tx_cache[tx['hash']] = tx
  
  def flush_tx_cache(self):
    with self.dlock:
      tx_insert_stmt = """INSERT OR IGNORE INTO txs(version,lock_time,hash,block,sequence)
                          VALUES (:version,:lock_time,:hash,:block,:sequence)"""
      
      txins_insert_stmt = """INSERT OR IGNORE INTO tx_ins(tx,out_hash,out_index,script,sequence)
                             VALUES(:hash,:out_hash,:out_index,:script,:sequence)"""
      
      txouts_insert_stmt = """INSERT OR IGNORE INTO tx_outs(tx,value,script)
                              VALUES(:hash,:value,:pk_script)"""

      for tx in self.tx_cache:
        print('Adding', tx)
        session.add(Transaction(tx))
      session.commit()
      import sys
      sys.exit()
      
      if len(self.tx_cache) > 0:
        c = self.db.cursor()
        for h,tx in self.tx_cache.items():
          c.execute(tx_insert_stmt,tx)
          for tx_in in tx['tx_ins']:
            tx_in['hash'] = tx['hash']
            tx_in['out_hash'] = tx_in['outpoint']['out_hash']
            tx_in['out_index'] = tx_in['outpoint']['out_index']
            c.execute(txins_insert_stmt,tx_in)
          for tx_out in tx['tx_outs']:
            tx_out['hash'] = tx['hash']
            c.execute(txouts_insert_stmt,tx_out)
        
        self.db.commit()
        self.tx_cache = {}
    
  def put_block(self,block):
    with self.dlock:
      if len(block['txs']) > 0:
        sequence = 0
        for tx in block['txs']:
          self.put_tx(tx,sequence,block['hash'])#the sequence tells us the order for merkle tree
          sequence += 1
          
      if block['hash'] not in self.block_cache:
        self.block_cache[block['hash']] = block
      
  def flush_block_cache(self):
    with self.dlock:
      block_insert_stmt = """INSERT OR IGNORE INTO blocks(version,prev_hash,merkle_root,timestamp,bits,nonce,hash,height)
                             VALUES(:version,:prev_hash,:merkle_root,:timestamp,:bits,:nonce,:hash,NULL)"""
                             
      if len(self.block_cache) > 0:
        c = self.db.cursor()
        c.executemany(block_insert_stmt,[v for k,v in self.block_cache.items()])
        self.db.commit()
        self.block_cache = {}
