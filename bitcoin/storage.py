import threading
import time

import bitcoin
import bitcoin.net.payload

from sqlalchemy import create_engine,Table,Column,MetaData,ForeignKey,DateTime,Integer,BigInteger,SmallInteger,Float
from sqlalchemy.types import BINARY
from sqlalchemy.orm import mapper,relationship, scoped_session, sessionmaker
from sqlalchemy.ext.orderinglist import ordering_list
from sqlalchemy.sql.expression import not_
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.exc import IntegrityError


engine = create_engine('sqlite:///bitcoin.sqlite3',echo=True)
#engine = create_engine('sqlite:///bitcoin.sqlite3')
metadata = MetaData()

blocks_table = Table('blocks',metadata,
  Column('id',Integer,primary_key=True),
  Column('hash',BINARY(32),unique=True),
  Column('prev_hash',BINARY(32),ForeignKey('blocks.hash'),nullable=True,unique=True),
  Column('merkle_root',BINARY(32)),
  Column('timestamp',DateTime,index=True),
  Column('bits',Integer,index=True),
  Column('nonce',BINARY(8)),
  Column('version',SmallInteger),
  Column('height',Float,nullable=True,index=True),
)

transactions_table = Table('transactions',metadata,
  Column('id',Integer,primary_key=True),
  Column('hash',BINARY(32),index=True,unique=True),
  Column('sequence',Integer,index=True,nullable=True),
  Column('version',SmallInteger),
  Column('lock_time',DateTime,index=True),
  Column('position',Integer,index=True,nullable=True),
  Column('block_id',Integer,ForeignKey('blocks.id'),nullable=True),
)

transaction_inputs_table = Table('transaction_inputs',metadata,
  Column('id',Integer,primary_key=True),
  Column('output_hash',BINARY(32),index=True),
  Column('output_index',Integer),
  Column('script',BINARY),
  Column('sequence',Integer),
  Column('position',Integer),
  Column('transaction_id',Integer,ForeignKey('transactions.id')),
)

transaction_outputs_table = Table('transaction_outputs',metadata,
  Column('id',Integer,primary_key=True),
  Column('value',BigInteger,index=True),
  Column('script',BINARY),
  Column('position',Integer),
  Column('transaction_id',Integer,ForeignKey('transactions.id')),
)

metadata.create_all(engine)

mapper(bitcoin.Block,blocks_table,properties={
  'prev_block': relationship(bitcoin.Block,primaryjoin=blocks_table.c.hash==blocks_table.c.prev_hash,remote_side=blocks_table.c.prev_hash),
  'next_blocks': relationship(bitcoin.Block,primaryjoin=blocks_table.c.hash==blocks_table.c.prev_hash,remote_side=blocks_table.c.hash),
  'transactions': relationship(bitcoin.Transaction,order_by=[transactions_table.c.sequence],collection_class=ordering_list('position')),
})
mapper(bitcoin.Transaction,transactions_table,properties={
  'inputs': relationship(bitcoin.TransactionInput,order_by=[transaction_inputs_table.c.position],collection_class=ordering_list('position')),
  'outputs': relationship(bitcoin.TransactionOutput,order_by=[transaction_outputs_table.c.position],collection_class=ordering_list('position')),
})

mapper(bitcoin.TransactionOutput,transaction_outputs_table)
mapper(bitcoin.TransactionInput,transaction_inputs_table)

session = scoped_session(sessionmaker(bind=engine))

class Storage(object):
  genesis_hash = b'o\xe2\x8c\n\xb6\xf1\xb3r\xc1\xa6\xa2F\xaec\xf7O\x93\x1e\x83e\xe1Z\x08\x9ch\xd6\x19\x00\x00\x00\x00\x00'
  
  def __init__(self):
    
    self.dlock = threading.RLock()

  def difficulty(self,bits):
    target = (bits & 0x00ffffff)*(2**(8*((bits >> 24) - 3))) 
    max_target = 0x00000000ffff0000000000000000000000000000000000000000000000000000
    return max_target/target
    
  def get_heads(self):
    with self.dlock:
      c = session.query(bitcoin.Block).filter(bitcoin.Block.height!=None).filter(not_(bitcoin.Block.hash.in_(session.query(bitcoin.Block.prev_hash))))
      return [h.hash for h in c]
      
  def get_tails(self):
    with self.dlock:
      c = session.query(bitcoin.Block).filter(bitcoin.Block.height!=None).filter(not_(bitcoin.Block.prev_hash.in_(session.query(bitcoin.Block.hash))))
      return [h.hash for h in c]
      
  def connect_blocks(self):
    with self.dlock:
      heads = self.get_heads()
      while heads:
        for head in heads:
          head.next_block.height = head.height + self.difficulty(head.next_block.bits)
        heads = self.get_heads()
      session.commit()
  
  def get_transaction(self,h):
    with self.dlock:
      try:
        return session.query(bitcoin.Transaction).filter_by(hash=h).one()
      except NoResultFound:
        return None
      
  def get_block(self,h):
    with self.dlock:
      try:
        return session.query(bitcoin.Block).filter_by(hash=h).one()
      except NoResultFound:
        return None
    
  def put_transaction(self,transaction):
    with self.dlock:
      session.add(transaction)
      try:
        session.commit()
      except IntegrityError as e:
        pass
    
  def put_block(self,block):
    with self.dlock:
      session.add(block)
      try:
        session.commit()
      except IntegrityError as e:
        pass
