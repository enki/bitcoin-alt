import sqlite3
import threading
import hashlib

import bitcoin.net.payload

class Storage:
  genesis_block = {'height': 1.0, 'nonce': b'\x1d\xac+|', 'version': 1, 'hash': b'o\xe2\x8c\n\xb6\xf1\xb3r\xc1\xa6\xa2F\xaec\xf7O\x93\x1e\x83e\xe1Z\x08\x9ch\xd6\x19\x00\x00\x00\x00\x00', 'txs': [{'tx_outs': [{'pk_script': b"A\x04g\x8a\xfd\xb0\xfeUH'\x19g\xf1\xa6q0\xb7\x10\\\xd6\xa8(\xe09\t\xa6yb\xe0\xea\x1fa\xde\xb6I\xf6\xbc?L\xef8\xc4\xf3U\x04\xe5\x1e\xc1\x12\xde\\8M\xf7\xba\x0b\x8dW\x8aLp+k\xf1\x1d_\xac", 'value': 5000000000}], 'lock_time': 0, 'version': 1, 'hash': None, 'tx_ins': [{'sequence': 4294967295, 'outpoint': {'out_index': 4294967295, 'out_hash': b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'}, 'script': b'\x04\xff\xff\x00\x1d\x01\x04EThe Times 03/Jan/2009 Chancellor on brink of second bailout for banks'}]}], 'prev_hash': b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00', 'timestamp': 1231006505, 'merkle_root': b';\xa3\xed\xfdz{\x12\xb2z\xc7,>gv\x8fa\x7f\xc8\x1b\xc3\x88\x8aQ2:\x9f\xb8\xaaK\x1e^J', 'bits': 486604799}
  
  def __init__(self):
    self.db = sqlite3.connect('bitcoin.sqlite3')
    self.db.row_factory = sqlite3.Row
    self.dlock = threading.RLock()
    self.tx_cache = {}
    self.block_cache = {}
    
  #  self.put_block(Storage.genesis_block)
  #  self.set_height(Storage.genesis_block['hash'],self.difficulty(Storage.genesis_block['bits']))
    
  def difficulty(self,bits):
    target = (bits & 0x00ffffff)*(2**(8*((bits >> 24) - 3))) 
    max_target = 0x00000000ffff0000000000000000000000000000000000000000000000000000
    return max_target/target
    
  def get_heads(self):
    with self.dlock:
      self.flush_block_cache()
      self.flush_tx_cache()
      c = self.db.execute('SELECT hash FROM blocks WHERE height IS NOT NULL AND hash NOT IN (SELECT prev_hash FROM blocks)')
      return [h[0] for h in c.fetchall()]
      
  def get_tails(self):
    with self.dlock:
      self.flush_block_cache()
      self.flush_tx_cache()
      c = self.db.execute('SELECT hash FROM blocks WHERE prev_hash NOT IN (SELECT hash FROM blocks) AND prev_hash IS NOT ?',(bytes.fromhex('0000000000000000000000000000000000000000000000000000000000000000'),))
      return [h[0] for h in c.fetchall()]
      
  def set_height(self,h,height):
    with self.dlock:
      self.flush_block_cache()
      self.flush_tx_cache()
      c = self.db.execute('UPDATE blocks SET height=? WHERE hash=?',(height,h))
      self.db.commit()
  
  def connect_blocks(self):
    with self.dlock:
      self.flush_block_cache()
      self.flush_tx_cache()
      c = self.db.execute('SELECT * FROM blocks WHERE height IS NULL AND prev_hash IN (SELECT hash FROM blocks WHERE height IS NOT NULL)')
      to_update = {}
      start_blocks = c.fetchall()
      
      for start_block in start_blocks:
          block = dict(start_block)
          while block:
            if block['prev_hash'] in to_update:
              prev_block = to_update[block['prev_hash']]
            else:
              prev_block = self.get_block(block['prev_hash'])
            
            block['height'] = self.difficulty(block['bits']) + prev_block['height']
            to_update[block['hash']] = block
                
            block = self.get_block_with_prev_hash(block['hash'])
      
      self.db.executemany('UPDATE blocks SET height=? WHERE hash=?',[(b['height'],b['hash']) for h,b in to_update.items()])
      self.db.commit()
  
  def get_tx(self,h):
    with self.dlock:
      if h in self.tx_cache:
        return self.tx_cache[h]
      else:
        c = self.db.execute('SELECT * FROM txs WHERE hash=?',(h,))
        r = c.fetchone()
        if r:
          tx = {}
          tx['hash'] = h
          tx['version'] = r['version']
          tx['lock_time'] = r['lock_time']
          tx['block_hash'] = r['block']
          tx['tx_ins'] = []
          tx['tx_outs'] = []
          
          c = self.db.execute('SELECT * FROM tx_ins WHERE tx=?',(h,))
          rows = c.fetchall()
          
          for r in rows:
            tx_in = {}
            tx_in['outpoint'] = {'out_index':r['out_index'],'out_hash':r['out_hash']}
            tx_in['script'] = r['script']
            tx_in['sequence'] = r['sequence']
            tx['tx_ins'].append(tx_in)
            
          c = self.db.execute('SELECT * FROM tx_outs WHERE tx=?',(h,))
          rows = c.fetchall()
          
          for r in rows:
            tx_out = {}
            tx_out['value'] = r['value']
            tx_out['pk_script'] = r['script']
            tx['tx_outs'].append(tx_out)
          
          return tx
        else:
          return None
        
  def get_block(self,h):
    with self.dlock:
      if h in self.block_cache:
        return self.block_cache[h]
      else:
        c = self.db.execute('SELECT * FROM blocks WHERE hash=?',(h,))
        r = c.fetchone()
        if r:
          return dict(r)
        else:
          return None
        
  def get_block_with_prev_hash(self,h):
    with self.dlock:
      self.flush_block_cache()
      self.flush_tx_cache()
      c = self.db.execute('SELECT * FROM blocks WHERE prev_hash=?',(h,))
      r = c.fetchone()
      if r:
        return dict(r)
      else:
        return None
    
  def put_tx(self,tx,sequence=None,block=None):
    with self.dlock:
      if tx['hash'] in self.tx_cache:
        return
      else:
        tx['block'] = block
        tx['sequence'] = sequence
        self.tx_cache[tx['hash']] = tx
        
        if len(self.tx_cache) > 1: # TODO entirely arbitrary
          self.flush_tx_cache()

  def flush_tx_cache(self):
    tx_insert_stmt = """INSERT OR IGNORE INTO txs(version,lock_time,hash,block,sequence)
                        VALUES (:version,:lock_time,:hash,:block,:sequence)"""
    
    txins_insert_stmt = """INSERT OR IGNORE INTO tx_ins(tx,out_hash,out_index,script,sequence)
                           VALUES(:hash,:out_hash,:out_index,:script,:sequence)"""
    
    txouts_insert_stmt = """INSERT OR IGNORE INTO tx_outs(tx,value,script)
                            VALUES(:hash,:value,:pk_script)"""
    with self.dlock:
      for h,tx in self.tx_cache.items():
        c = self.db.cursor()
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
      if block['hash'] in self.block_cache:
        return
      else:
        self.block_cache[block['hash']] = block
        
        if len(block['txs']) > 0:
          sequence = 0
          for tx in block['txs']:
            self.put_tx(tx,sequence,block['hash'])#the sequence tells us the order for merkle tree
            sequence += 1
        
        if len(self.block_cache) > 1: # TODO entirely arbitrary
          self.flush_block_cache()
  
  def flush_block_cache(self):
    with self.dlock:
      c = self.db.cursor()
      c.executemany("""INSERT OR IGNORE INTO blocks(version,prev_hash,merkle_root,timestamp,bits,nonce,hash,height)
                       VALUES(:version,:prev_hash,:merkle_root,:timestamp,:bits,:nonce,:hash,NULL)""",self.block_cache)
      self.db.commit()
      self.block_cache = {}
