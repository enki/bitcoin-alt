import threading
import time

import bitcoin
import bitcoin.net.payload

from sqlalchemy import create_engine,Table,Column,MetaData,ForeignKey,DateTime,Integer,BigInteger,SmallInteger,Float
from sqlalchemy.types import BINARY
from sqlalchemy.orm import mapper,relationship, scoped_session, sessionmaker,backref
from sqlalchemy.ext.orderinglist import ordering_list
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.exc import IntegrityError


#engine = create_engine('sqlite:///bitcoin.sqlite3',echo=True,connect_args={'timeout':5000})
engine = create_engine('sqlite:///bitcoin.sqlite3',connect_args={'timeout':5000})
#engine = create_engine('postgresql+psycopg2://bitcoin@localhost/bitcoin')
metadata = MetaData()

blocks_table = Table('blocks',metadata,
  Column('hash',BINARY(32),primary_key=True),
  Column('prev_hash',BINARY(32)),
  Column('merkle_root',BINARY(32)),
  Column('timestamp',Integer),
  Column('bits',Integer),
  Column('nonce',BINARY(8)),
  Column('version',SmallInteger),
  Column('height',Float,nullable=True,index=True),
)

transactions_table = Table('transactions',metadata,
  Column('hash',BINARY(32),primary_key=True),
  Column('version',SmallInteger),
  Column('lock_time',Integer),
  Column('position',Integer,nullable=True),
  Column('block_hash',BINARY(32),ForeignKey('blocks.hash'),nullable=True),
)

transaction_inputs_table = Table('transaction_inputs',metadata,
  Column('id',Integer,primary_key=True),
  Column('output_hash',BINARY(32),index=True),
  Column('output_index',Integer),
  Column('script',BINARY),
  Column('sequence',Integer),
  Column('position',Integer),
  Column('transaction_hash',BINARY(32),ForeignKey('transactions.hash')),
)

transaction_outputs_table = Table('transaction_outputs',metadata,
  Column('id',Integer,primary_key=True),
  Column('value',BigInteger),
  Column('script',BINARY),
  Column('position',Integer),
  Column('transaction_hash',BINARY(32),ForeignKey('transactions.hash')),
)

metadata.create_all(engine)

mapper(bitcoin.Block,blocks_table,properties={
  'prev_block': relationship(bitcoin.Block,
                foreign_keys=blocks_table.c.prev_hash,
                primaryjoin=blocks_table.c.hash==blocks_table.c.prev_hash,
                remote_side=blocks_table.c.hash,
                backref=backref('next_blocks')),
  'transactions': relationship(bitcoin.Transaction,
                order_by=[transactions_table.c.position],
                collection_class=ordering_list('position'),
                backref=backref('block')),
})
mapper(bitcoin.Transaction,transactions_table,properties={
  'inputs': relationship(bitcoin.TransactionInput,
              order_by=[transaction_inputs_table.c.position],
              collection_class=ordering_list('position'),
              backref=backref('transaction')),
  'outputs': relationship(bitcoin.TransactionOutput,
              order_by=[transaction_outputs_table.c.position],
              collection_class=ordering_list('position'),
              backref=backref('transaction')),
})

mapper(bitcoin.TransactionOutput,transaction_outputs_table)
mapper(bitcoin.TransactionInput,transaction_inputs_table)

session = scoped_session(sessionmaker(bind=engine))
session.execute('PRAGMA journal_mode=WAL;')
flush_lock = threading.RLock()

genesis_hash = b'o\xe2\x8c\n\xb6\xf1\xb3r\xc1\xa6\xa2F\xaec\xf7O\x93\x1e\x83e\xe1Z\x08\x9ch\xd6\x19\x00\x00\x00\x00\x00'
