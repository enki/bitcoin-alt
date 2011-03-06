import sqlite3
import threading

class Storage:
  def __init__(self):
    self.db = sqlite3.connect('bitcoin.sqlite3')
    self.dlock = threading.RLock()
    
  def get_tx(self,h):
    with self.dlock:
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
          tx_in['out_hash'] = r['out_hash']
          tx_in['out_index'] = r['out_index']
          tx_in['script'] = r['script']
          tx_in['sequence'] = r['sequence']
          tx['tx_ins'].append(tx_in)
          
        c = self.db.execute('SELECT * FROM tx_outs WHERE tx=?',(h,))
        rows = c.fetchall()
        
        for r in rows:
          tx_out = {}
          tx_out['value'] = r['value']
          tx_out['script'] = r['script']
          tx['tx_outs'].append(tx_out)
        
        return tx
      else:
        return None
        
  def get_block(self,h):
    with self.dlock:
      c = self.db.execute('SELECT * FROM blocks WHERE hash=?',(h,))
      r = c.fetchone()
      if r:
        block = {}
        block['hash'] = h
        block['version'] = r['version']
        block['prev_block'] = r['prev_block']
        block['merkle_root'] = r['merkle_root']
        block['timestamp'] = r['timestamp']
        block['bits'] = r['bits']
        block['nonce'] = r['nonce']
        
        return block
      else:
        return None
    
  def put_tx(self,tx,h,block=None):
    with self.dlock:
      c = self.db.cursor()
      c.execute('INSERT INTO txs(version,lock_time,hash,block) VALUES (?,?,?,?)',tx['version'],tx['lock_time'],tx['hash'],block)
      for tx_in in tx['tx_ins']:
        c.execute('INSERT INTO tx_ins(tx,out_hash,out_index,script,sequence) VALUES(?,?,?,?,?)',tx['hash'],tx_in['out_hash'],tx_in['out_index'],tx_in['script'],tx_in['sequence'])
        
      for tx_out in tx['tx_outs']:
        c.execute('INSERT INTO tx_outs(tx,value,script) VALUES(?,?,?)',tx['hash'],tx_out['value'],tx_out['script'])
        
      self.db.commit()
      
  def put_block(self,block):
    with self.dlock:
      c = self.db.cursor()
      c.execute('INSERT INTO blocks(version,prev_hash,merkle_root,timestamp,bits,nonce) VALUES(?,?,?,?,?,?)',block['version'],block['prev_hash'],block['merkle_root'],block['timestamp'],block['bits'],block['nonce'])
      self.db.commit()
