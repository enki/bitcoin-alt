import struct
import socket
import hashlib

import bitcoin

class buffer_builder:
  def __init__(self):
    self.buffer = b''
    
  def write(self,buf):
    self.buffer += buf
  
  def pack(self,format,args):
    self.write(struct.pack(format,*args))
    
  def uint8(self,i,little_endian=True):
    if little_endian:
      format = '<B'
    else:
      format = '>B'
    self.pack(format,(i,))

  def uint16(self,i,little_endian=True):
    if little_endian:
      format = '<H'
    else:
      format = '>H'
    self.pack(format,(i,))
    
  def uint32(self,i,little_endian=True):
    if little_endian:
      format = '<I'
    else:
      format = '>I'
    self.pack(format,(i,))
    
  def uint64(self,i,little_endian=True):
    if little_endian:
      format = '<Q'
    else:
      format = '>Q'
    self.pack(format,(i,))
  
  def var_uint(self,i):
    if i < 0xFD:
      self.pack('<B',(i,))
    elif i <= 2**16:
      self.write(b'\xfd')
      self.pack('<H',(i,))
    elif i <= 2**32:
      self.write(b'\xfe')
      self.pack('<I',(i,))
    elif i <= 2**64:
      self.write(b'\xff')
      self.pack('<Q',(i,))
    else:
      raise Exception("integer wayyyy too big")
  
  def string(self,string):
    self.var_uint(len(string))
    self.write(string)
    
  def null_string(self,string):
    self.write(string)
    self.write(b'\x00')
    
  def fixed_string(self,string,length):
    if len(string) > length:
      raise Exception("string too long")
    self.write(string)
    self.write(b'\x00'*(length-len(string)))
    
  def addr(self,address):
    self.uint64(address.services)
    self.write(socket.inet_pton(socket.AF_INET6,address.addr))
    self.uint16(address.port,little_endian=False)
    
  def inv(self,inv):
    if len(inv['hash']) != 32:
      raise Exception("hash length is wrong")
    
    self.uint32(inv['type'])
    self.write(inv['hash'])
    
  def input(self,input):
    if len(input.output_hash) != 32:
      raise Exception("hash length is wrong")
    
    self.write(input.output_hash)
    self.uint32(input.output_index)
    self.var_uint(len(input.script))
    self.write(input.script)
    self.uint32(input.sequence)
    
  def output(self,output):
    self.uint64(output.value)
    self.var_uint(len(output.script))
    self.write(output.script)
    
  def transaction(self,transaction):
    self.uint32(transaction.version)
    self.var_uint(len(transaction.inputs))
    for tx_in in transaction.inputs:
      self.input(tx_in)
    self.var_uint(len(transaction.outputs))
    for tx_out in transaction.outputs:
      self.output(tx_out)
    self.uint32(transaction.lock_time)
    
def version(version,
            services,
            timestamp,
            addr_me,
            addr_you=None,
            nonce=None,
            sub_version_num=None,
            start_height=None):
            
  b = buffer_builder()
  
  b.uint32(version)
  b.uint64(services)
  b.uint64(timestamp)
  
  b.addr(addr_me)
  
  if version < 106:
    return b.buffer
  else:
    b.addr(addr_you)
    
    if len(nonce) != 8:
      raise Exception("len(nonce) != 8")
      
    b.write(nonce)
    b.null_string(sub_version_num.encode('ascii'))
    
    if version < 209:
      return b.buffer
    else:
      b.uint32(start_height)
      return b.buffer
      
def addr(addrs,version):
  b = buffer_builder()
  b.var_uint(len(addrs))
  for addr in addrs:
    if version >= 31402:
      b.uint32(addr['timestamp'])
    b.addr(addr)
  return b.buffer

def inv(invs,version):
  b = buffer_builder()
  b.var_uint(len(invs))
  for inv in invs:
    b.inv(inv)
  return b.buffer
  
def getdata(invs,version):
  b = buffer_builder()
  b.var_uint(len(invs))
  for inv in invs:
    b.inv(inv)
  return b.buffer

def getblocks(version,starts,stop):
  b = buffer_builder()
  b.uint32(version)
  b.var_uint(len(starts))
  for start in starts:
    b.write(start)
  b.write(stop)
  return b.buffer
  
def getheaders(version,starts,stop):
  b = buffer_builder()
  b.uint32(version)
  b.var_uint(len(starts))
  for start in starts:
    b.write(start)
  b.write(stop)
  return b.buffer
  
def transaction(transaction):
  b = buffer_builder()
  b.transaction(transaction)
  return b.buffer
  
def block(block):
  b = buffer_builder()
  b.uint32(block.version)
  b.write(block.prev_hash)
  b.write(block.merkle_root)
  b.uint32(block.timestamp)
  b.uint32(block.bits)
  b.write(block.nonce)
  for tx in block.transactions:
    b.transaction(tx)
  return b.buffer

class buffer_parser:
  def __init__(self,buf):
    self.buffer = buf
    self.offset = 0
    
  def read(self,length):
    ret = self.buffer[self.offset:self.offset+length]
    self.offset += length
    return ret
    
  def read_delim(self,delim):
    i = self.buffer[self.offset:].find(delim)
    ret = self.buffer[self.offset:][:i+len(delim)]
    self.offset += i + len(delim)
    return ret
    
  def unpack(self,format):
    return struct.unpack(format,self.read(struct.calcsize(format)))

  def uint8(self,little_endian=True):
    if little_endian:
      format = '<B'
    else:
      format = '>B'
    return self.unpack(format)[0]

  def uint16(self,little_endian=True):
    if little_endian:
      format = '<H'
    else:
      format = '>H'
    return self.unpack(format)[0]
    
  def uint32(self,little_endian=True):
    if little_endian:
      format = '<I'
    else:
      format = '>I'
    return self.unpack(format)[0]
    
  def uint64(self,little_endian=True):
    if little_endian:
      format = '<Q'
    else:
      format = '>Q'
    return self.unpack(format)[0]
  
  def var_uint(self):
    first_byte = self.uint8()
    if first_byte < 0xfd:
      return first_byte
    else:
      format = {0xfd: '<H', 0xfe: '<I', 0xff: '<L'}[first_byte]
      return self.unpack(format)[0]
  
  def string(self):
    length = self.var_uint()
    return self.read(length).decode('ascii')
    
  def null_string(self):
    return self.read_delim(b'\x00').decode('ascii').strip('\x00')
    
  def fixed_string(self,length):
    return self.read(length).decode('ascii').strip('\x00')

  def addr(self):
    services = self.uint64()
    addr = socket.inet_ntop(socket.AF_INET6,self.read(16))
    port = self.uint16(False)
    return bitcoin.Address(addr,port,services)
    
  def inv_vect(self):
    inv_type = self.uint32()
    inv_hash = self.read(32)
    return {'type':inv_type,'hash':inv_hash}
    
class parser:
  def __init__(self):
    self.version = None
  
  def parse(self,command,p):
    self.helper = buffer_parser(p)
    return {
    'version':self.parse_version,
    'verack':self.parse_verack,
    'addr':self.parse_addr,
    'inv':self.parse_inv,
    'getdata':self.parse_inv,
    'getblocks':self.parse_getblocks,
    'getheaders':self.parse_getblocks,
    'tx':self.parse_tx,
    'block':self.parse_block,
    'getaddr':self.parse_getaddr,
    'checkorder':self.parse_checkorder,
    'submitorder':self.parse_submitorder,
    'reply':self.parse_reply,
    'ping':self.parse_ping,
    'alert':self.parse_alert,
    }[command]()
  
  def parse_version(self):
    version = self.helper.uint32()
    
    self.version = version
    
    services = self.helper.uint64()
    timestamp = self.helper.uint64()
    
    addr_me = self.helper.addr()
    
    ret = {'version':version,
           'services':services,
           'timestamp':timestamp,
           'addr_me':addr_me}
    
    if version < 106:
      return ret
    else:
      addr_you = self.helper.addr()
      nonce = self.helper.read(8)
      sub_version_num = self.helper.null_string()
      
      ret.update({'addr_you':addr_you,
                  'nonce':nonce,
                  'sub_version_num':sub_version_num})
      
      if version < 209:
        return ret
      else:
        start_height = self.helper.uint32()
        ret.update({'start_height':start_height})
    return ret
    
  def parse_verack(self):
    return None
    
  def parse_addr(self):
    count = self.helper.var_uint()
    addrs = []
    for x in range(count):
      if self.version >= 31402:
        timestamp = self.helper.uint32()
        addr = self.helper.addr()
        addrs.append(addr)
      else:
        addr = self.helper.addr()
        addrs.append(addr)
    return addrs
    
  def parse_inv(self):
    count = self.helper.var_uint()
    invs = []
    for x in range(count):
      invs.append(self.helper.inv_vect())
    return invs
    
  def parse_getblocks(self):
    version = self.helper.uint32()
    count = self.helper.var_uint()
    starts = []
    for x in range(count):
      starts.append(self.helper.read(32))
    stop = self.helper.read(32)
    return {'version':version,'starts':starts,'stop':stop}

  def parse_txin(self):
    out_hash = self.helper.read(32)
    out_index = self.helper.uint32()
    script_length = self.helper.var_uint()
    script = self.helper.read(script_length)
    sequence = self.helper.uint32()
    return bitcoin.TransactionInput(out_hash,out_index,script,sequence)
    
  def parse_txout(self):
    value = self.helper.uint64()
    script_length = self.helper.var_uint()
    script = self.helper.read(script_length)
    return bitcoin.TransactionOutput(value,script)
    
  def parse_tx(self):
    start = self.helper.offset
    version = self.helper.uint32()
    tx_in_count = self.helper.var_uint()
    tx_ins = []
    for x in range(tx_in_count):
      tx_ins.append(self.parse_txin())
    tx_out_count = self.helper.var_uint()
    tx_outs = []
    for x in range(tx_out_count):
      tx_outs.append(self.parse_txout())
    lock_time = self.helper.uint32()
    
    end = self.helper.offset
    
    h = hashlib.sha256(hashlib.sha256(self.helper.buffer[start:end]).digest()).digest()
    
    t = bitcoin.Transaction(h,version,lock_time)
    for tx_in in tx_ins:
      t.inputs.append(tx_in)
    for tx_out in tx_outs:
      t.outputs.append(tx_out)
    return t

  def parse_block(self):
    h = hashlib.sha256(hashlib.sha256(self.helper.buffer[:4+32+32+4+4+4]).digest()).digest()
    version = self.helper.uint32()
    prev_hash = self.helper.read(32)
    merkle_root = self.helper.read(32)
    timestamp = self.helper.uint32()
    bits = self.helper.uint32()
    nonce = self.helper.read(4)
    tx_count = self.helper.var_uint()
    txs = []
    for x in range(tx_count):
      txs.append(self.parse_tx())
    
    b = bitcoin.Block(h,prev_hash,merkle_root,timestamp,bits,nonce,version)
    for tx in txs:
      b.transactions.append(tx)
      
    return b
    
  def parse_getaddr(self):
    return None
    
  def parse_checkorder(self):
    return False
    
  def parse_submitorder(self):
    return False
    
  def parse_reply(self):
    return False
  
  def parse_ping(self):
    return None
    
  def parse_alert(self):
    message = self.helper.string()
    signature = self.helper.string()
    return {'message':message,'signature':signature}
