import threading
import time
import sqlite3

import bitcoin
import bitcoin.net.payload

create_statements = ["""CREATE TABLE IF NOT EXISTS blocks (
	hash BINARY(32) NOT NULL, 
	prev_hash BINARY(32) NOT NULL,
	merkle_root BINARY(32) NOT NULL,
	timestamp INTEGER NOT NULL,
	bits INTEGER NOT NULL,
	nonce BINARY(8) NOT NULL,
	version SMALLINT NOT NULL,
	height FLOAT,
	PRIMARY KEY (hash)
);""",
"""CREATE TABLE IF NOT EXISTS transaction_inputs (
	output_hash BINARY(32) NOT NULL,
	output_index INTEGER NOT NULL,
	script BINARY NOT NULL,
	sequence INTEGER NOT NULL,
	position INTEGER NOT NULL,
	transaction_hash BINARY(32) NOT NULL,
	PRIMARY KEY (output_hash,output_index),
	FOREIGN KEY(transaction_hash) REFERENCES transactions (hash)
);""",
"""CREATE TABLE IF NOT EXISTS transaction_outputs (
	value BIGINT NOT NULL,
	script BINARY NOT NULL,
	position INTEGER NOT NULL,
	transaction_hash BINARY(32) NOT NULL,
	PRIMARY KEY (transaction_hash,position), 
	FOREIGN KEY(transaction_hash) REFERENCES transactions (hash)
);""",
"""CREATE TABLE IF NOT EXISTS transactions (
	hash BINARY(32) NOT NULL,
	version SMALLINT NOT NULL,
	lock_time INTEGER NOT NULL,
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
    
    for create_statement in create_statements:
      self.db.execute(create_statement)
    self.db.commit()
    
  def get_block(self,hash):
    blocks = self.get_blocks((hash,))
    if len(blocks) == 1:
      return blocks[0]
    else:
      return None
  
  def get_blocks(self,hashes):
    blocks = []
    for hash in hashes:
      c = self.db.execute('SELECT * FROM blocks WHERE hash=?',(hash,))
      row = c.fetchone()
      if row:
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
    for block in blocks:
      self.db.execute('INSERT OR IGNORE INTO blocks(hash,prev_hash,merkle_root,timestamp,bits,nonce,version,height) VALUES(?,?,?,?,?,?,?,?)',(block.hash,block.prev_hash,block.merkle_root,block.timestamp,block.bits,block.nonce,block.version,block.height))
      self.put_transactions(block.transactions,False)
      
    self.connect_blocks()
    self.db.commit()
    
  def put_block(self,block):
    self.put_blocks([block])
    
  def set_height(self,hash,height):
    self.set_heights([(height,hash)])
    
  def set_heights(self,heights):# heights = [(height,hash)]
    self.db.executemany('UPDATE blocks SET height=? WHERE hash=?',heights)
    self.db.commit()
    
  def get_transaction(self,hash):
    transactions = self.get_transactions([hash])
    if len(transactions) == 1:
      return transactions[0]
    else:
      return None
    
  def get_transactions(self,hashes):
    transactions = []
    for hash in hashes:
      c = self.db.execute('SELECT * FROM transactions WHERE hash=?',(hash,))
      row = c.fetchone()
      if row:
        transaction = bitcoin.Transaction(row['hash'],row['version'],row['lock_time'])
        
        c = self.db.execute('SELECT * FROM transaction_inputs WHERE transaction_hash=? ORDER BY position',(transaction.hash,))
        rows = c.fetchall()
        for row in rows:
          input = bitcoin.TransactionInput(row['output_hash'],row['output_index'],row['script'],row['sequence'])
          transaction.inputs.append(input)
        
        c = self.db.execute('SELECT * FROM transaction_outputs WHERE transaction_hash=? ORDER BY position',(transaction.hash,))
        rows = c.fetchall()
        for row in rows:
          output = bitcoin.TransactionOutput(row['value'],row['script'])
          transaction.outputs.append(output)
        
        transactions.append(transaction)
    return transactions
  
  def put_transaction(self,transaction,commit=True):
    self.put_transactions([transaction],commit)
    
  def put_transactions(self,transactions,commit=True):
    for transaction in transactions:
      self.db.execute('INSERT OR IGNORE INTO transactions(hash,version,lock_time,position,block_hash) VALUES(?,?,?,?,?)',(transaction.hash,transaction.version,transaction.lock_time,transaction.position,transaction.block_hash))
      for input in transaction.inputs:
        self.db.execute('INSERT OR IGNORE INTO transaction_inputs(output_hash,output_index,script,sequence,position,transaction_hash) VALUES(?,?,?,?,?,?)',(input.hash,input.index,input.script,input.sequence,transaction.inputs.index(input),transaction.hash))
      for output in transaction.outputs:
        self.db.execute('INSERT OR IGNORE INTO transaction_outputs(value,script,position,transaction_hash) VALUES(?,?,?,?)',(output.value,output.script,transaction.outputs.index(output),transaction.hash))
    if commit:
      self.db.commit()
    
  def connect_blocks(self):
    heads = self.heads()
    heights = []
    while heads:
      head = heads.pop()
      next_blocks = self.next_blocks(head)
      if next_blocks:
        for next_block in next_blocks:
          next_block.height = head.height + next_block.difficulty()
          heights.append((next_block.height,next_block.hash))
          heads.append(next_block)
    
    self.set_heights(heights)
