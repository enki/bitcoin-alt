import threading
import time
import sqlite3

import bitcoin
import bitcoin.net.payload

create_statements = ["""CREATE TABLE IF NOT EXISTS blocks (
	hash BINARY(32) NOT NULL, 
	prev_hash BINARY(32), 
	merkle_root BINARY(32), 
	timestamp INTEGER, 
	bits INTEGER, 
	nonce BINARY(8), 
	version SMALLINT, 
	height FLOAT, 
	PRIMARY KEY (hash)
);""",
"""CREATE TABLE IF NOT EXISTS transaction_inputs (
	output_hash BINARY(32), 
	output_index INTEGER, 
	script BINARY, 
	sequence INTEGER, 
	position INTEGER, 
	transaction_hash BINARY(32), 
	PRIMARY KEY (output_hash,output_index), 
	FOREIGN KEY(transaction_hash) REFERENCES transactions (hash)
);""",
"""CREATE TABLE IF NOT EXISTS transaction_outputs (
	value BIGINT, 
	script BINARY, 
	position INTEGER, 
	transaction_hash BINARY(32), 
	PRIMARY KEY (transaction_hash,position), 
	FOREIGN KEY(transaction_hash) REFERENCES transactions (hash)
);""",
"""CREATE TABLE IF NOT EXISTS transactions (
	hash BINARY(32) NOT NULL, 
	version SMALLINT, 
	lock_time INTEGER, 
	position INTEGER, 
	block_hash BINARY(32), 
	PRIMARY KEY (hash), 
	FOREIGN KEY(block_hash) REFERENCES blocks (hash)
);"""]

genesis_hash = b'o\xe2\x8c\n\xb6\xf1\xb3r\xc1\xa6\xa2F\xaec\xf7O\x93\x1e\x83e\xe1Z\x08\x9ch\xd6\x19\x00\x00\x00\x00\x00'

class Storage:
  def __init__(self):
    super(Storage,self).__init__()
    
    self.db = sqlite3.connect('bitcoin.sqlite3')
    self.db.row_factory = sqlite3.Row
    self.db.execute('PRAGMA journal_mode=WAL;')
    
  def get_block(self,hash):
    blocks = self.get_blocks((hash,))
    if blocks == 1:
      return blocks[0]
    else:
      return None
  
  def get_blocks(self,hashes):
    blocks = []
    for hash in hashes:
      c = self.db.execute('SELECT * FROM blocks WHERE hash=?',(hash,))
      row = c.fetchone()
      block = bitcoin.Block(**row)
      blocks.append(block)
    return blocks
    
  def heads(self):
    c = self.db.execute('SELECT * FROM blocks WHERE height IS NOT NULL AND hash NOT IN (SELECT prev_hash FROM blocks WHERE height IS NOT NULL)')
    return [bitcoin.Block(**block) for block in c.fetchall()]
    
  def next_blocks(self,block):
    c = self.db.execute('SELECT * FROM blocks WHERE prev_hash=?',(block.hash,))
    return [bitcoin.Block(**block) for block in c.fetchall()]
    
  def put_blocks(self,blocks):
    self.db.executemany('INSERT OR IGNORE INTO blocks(hash,prev_hash,merkle_root,timestamp,bits,nonce,version) VALUES(:hash,:prev_hash,:merkle_root,:timestamp,:bits,:nonce,:version)',blocks)
    self.db.commit()
    
  def put_block(self,block):
    self.put_blocks([block])
    
