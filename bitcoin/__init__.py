class Address(object):
  def __init__(self,host,port):
    self.host = host
    self.port = port

class Block(object):
  def __init__(self,block_hash,prev_hash,merkle_root,timestamp,bits,nonce,version):
    self.hash = block_hash
    self.prev_hash = prev_hash
    self.merkle_root = merkle_root
    self.timestamp = timestamp
    self.bits = bits
    self.nonce = none
    self.version = version
    
class Tx(object): 
  def __init__(self,tx_hash,sequence,version,lock_time):
    self.hash = tx_hash
    self.sequence = sequence
    self.version = version
    self.lock_time = lock_time
    
    self.tx_ins = []
    self.tx_outs = []

class TxOut(object): 
  def __init__(self,value,script):
    self.value = value
    self.script = script
  
class TxIn(object):  
  def __init__(self,out_hash,out_index,script):
    self.out_hash = out_hash
    self.out_index = out_index
    self.script = script
