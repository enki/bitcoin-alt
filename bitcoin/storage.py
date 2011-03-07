import sqlite3
import threading

class Storage:
  def __init__(self):
    self.db = sqlite3.connect('bitcoin.sqlite3')
    self.db.row_factory = sqlite3.Row
    self.dlock = threading.RLock()
    
  def difficulty(bits):
    target = (bits & 0x00ffffff)*(2**(8*((bits >> 24) - 3))) 
    max_target = 0x00000000ffff0000000000000000000000000000000000000000000000000000
    return max_target/target
  
  def get_needed(self):
    with self.dlock:
      needed = set()
      c = self.db.execute('SELECT * FROM blocks WHERE height IS NULL')
      for block in c:
        prev_block = self.get_block(block['prev_hash'])
        if prev_block:
          if prev_block['height']:
            self.db.execute('UPDATE blocks SET height=? WHERE hash=?',(prev_block['height']+difficulty(block['bits']),block['hash']))
        else:
          needed.add(block['prev_hash'])
      return needed
  
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
      c = self.db.execute('SELECT * FROM blocks WHERE hash=?',(h,))
      r = c.fetchone()
      if r:
        block = {}
        block['hash'] = h
        block['version'] = r['version']
        block['prev_hash'] = r['prev_hash']
        block['merkle_root'] = r['merkle_root']
        block['timestamp'] = r['timestamp']
        block['bits'] = r['bits']
        block['nonce'] = r['nonce']
        
        return block
      else:
        return None
    
  def put_tx(self,tx,sequence=None,block=None):
    if not tx['hash']:
      tx['hash'] = hashlib.sha256(hashlib.sha256(bitcoin.net.payload.tx(tx['version'],tx['tx_ins'],tx['tx_outs'],tx['lock_time'])).digest()).digest()
    if not self.get_tx(tx['hash']):
      with self.dlock:
        c = self.db.cursor()
        c.execute('INSERT INTO txs(version,lock_time,hash,block,sequence) VALUES (?,?,?,?,?)',(tx['version'],tx['lock_time'],tx['hash'],block,sequence))
        for tx_in in tx['tx_ins']:
          c.execute('INSERT INTO tx_ins(tx,out_hash,out_index,script,sequence) VALUES(?,?,?,?,?)',(tx['hash'],tx_in['outpoint']['out_hash'],tx_in['outpoint']['out_index'],tx_in['script'],tx_in['sequence']))
          
        for tx_out in tx['tx_outs']:
          c.execute('INSERT INTO tx_outs(tx,value,script) VALUES(?,?,?)',(tx['hash'],tx_out['value'],tx_out['pk_script']))
          
        self.db.commit()
      
  def put_block(self,block):
    if not block['hash']:
      block['hash'] = hashlib.sha256(hashlib.sha256(bitcoin.net.payload.block(block['version'],block['prev_hash'],block['merkle_root'],block['timestamp'],block['bits'].block['nonce'],[])).digest()).digest()
    if not self.get_block(block['hash']):
      with self.dlock:
        for x in range(len(block['txs'])):# TODO these should be part of the same tx, oh well
          self.put_tx(block['txs'][x],x,block['hash'])
        c = self.db.cursor()
        c.execute('INSERT INTO blocks(version,prev_hash,merkle_root,timestamp,bits,nonce,hash,height) VALUES(:version,:prev_hash,:merkle_root,:timestamp,:bits,:nonce,:hash,NULL)',block)
        self.db.commit()
