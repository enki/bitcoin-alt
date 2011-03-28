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
	id INTEGER NOT NULL, 
	output_hash BINARY(32), 
	output_index INTEGER, 
	script BINARY, 
	sequence INTEGER, 
	position INTEGER, 
	transaction_hash BINARY(32), 
	PRIMARY KEY (id), 
	FOREIGN KEY(transaction_hash) REFERENCES transactions (hash)
);""",
"""CREATE TABLE IF NOT EXISTS transaction_outputs (
	id INTEGER NOT NULL, 
	value BIGINT, 
	script BINARY, 
	position INTEGER, 
	transaction_hash BINARY(32), 
	PRIMARY KEY (id), 
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
);""",
"""CREATE INDEX IF NOT EXISTS ix_blocks_height ON blocks (height);""",
"""CREATE INDEX IF NOT EXISTS ix_transaction_inputs_output_hash ON transaction_inputs (output_hash);"""]

genesis_hash = b'o\xe2\x8c\n\xb6\xf1\xb3r\xc1\xa6\xa2F\xaec\xf7O\x93\x1e\x83e\xe1Z\x08\x9ch\xd6\x19\x00\x00\x00\x00\x00'

class Storage:
  def __init__(self):
    super(Storage,self).__init__()
    
    self.db = sqlite3.connect('bitcoin.sqlite3')
    self.db.execute('PRAGMA journal_mode=WAL;')
    
  def get_block(self,hash):
    c = self.db.execute('SELECT * FROM blocks WHERE hash=?',(hash,))
  
  def get_blocks(self,hashes):
    c = self.db.execute('SELECT * FROM blocks WHERE hash=?
    
