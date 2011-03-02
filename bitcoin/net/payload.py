import struct

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
    self.write(b'\xff')
    self.pack('<Q',(i,))
  
  def string(self,s):
    self.var_uint(len(s))
    self.write(s)
    
  def null_string(self,s):
    self.write(s)
    self.write(b'\x00')
    
  def fixed_string(self,s,length):
    if len(s) > length:
      raise Exception("string too long")
    self.write(s)
    self.write(b'\x00'*(length-len(s)))
    
  def addr(self,services,addr,port):
    if len(addr) != 16:
      raise Exception("addr length is wrong")
    
    self.uint64(services)
    self.write(addr)
    self.uint16(port,little_endian=False)
    
  def inv(self,t,h):
    if len(h) != 32:
      raise Exception("hash length is wrong")
    
    self.uint32(t)
    self.write(h)
    
  def outpoint(self,h,i):
    if len(h) != 32:
      raise Exception("hash length is wrong")
    
    self.write(h)
    self.uint32(i)
    
  def tx_in(self,outpoint,script,sequence):
    self.outpoint(*outpoint)
    self.var_uint(len(script))
    self.write(script)
    self.uint32(sequence)
    
  def tx_out(self,value,pk_script):
    self.uint64(value)
    self.var_uint(len(pk_script))
    self.write(pk_script)
    
  def tx(self,version,tx_ins,tx_outs,lock_time):
    b = buffer_builder()
    b.uint32(version)
    b.var_uint(len(tx_ins))
    for tx_in in tx_ins:
      b.tx_in(tx_in)
    b.var_uint(len(tx_outs))
    for tx_out in tx_outs:
      b.tx_out(tx_out)
    b.uint32(lock_time)
    return b.buffer
    
def version(version,services,timestamp,addr_me,addr_you=None,nonce=None,sub_version_num=None,start_height=None):
  b = buffer_builder()
  b.uint32(version)
  b.uint64(services)
  b.uint64(timestamp)
  
  b.addr(*addr_me)
  
  if version < 106:
    return b.buffer
  else:
    b.addr(*addr_you)
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
    b.inv(*inv)
  return b.buffer
  
def getdata(invs,version):
  b = buffer_builder()
  b.var_uint(len(invs))
  for inv in invs:
    b.inv(*inv)
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
  
def tx(version,tx_ins,tx_outs,lock_time):
  b = buffer_builder()
  b.tx(version,tx_ins,tx_outs,lock_time)
  return b.buffer
  
def block(version,prev_block,merkle_root,timestamp,bits,nonce,txs):
  b = buffer_builder()
  b.uint32(version)
  b.write(prev_block)
  b.write(merkle_root)
  b.uint32(timestamp)
  b.write(bits)
  b.write(nonce)
  for tx in txs:
    b.tx(*tx)
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
    
class parser:
  def __init__(self):
    self.version = None
  
  def parse(self,command,p):
    try:
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
    except KeyError as e:
      print(e)
      return False

  def read_addr(self):
    services = self.helper.uint64()
    addr = self.helper.read(16)
    port = self.helper.uint16(False)
    return (services,addr,port)
    
  def read_inv_vect(self):
    inv_type = self.helper.uint32()
    inv_hash = self.helper.read(32)
    return (inv_type,inv_hash)
  
  def parse_version(self):
    version = self.helper.uint32()
    
    self.version = version
    
    services = self.helper.uint64()
    timestamp = self.helper.uint64()
    
    addr_me = self.read_addr()
    
    ret = {'version':version,'services':services,'timestamp':timestamp,'addr_me':addr_me}
    if version < 106:
      return ret
    else:
      addr_you = self.read_addr()
      nonce = self.helper.read(8)
      sub_version_num = self.helper.null_string()
      
      ret.update({'addr_you':addr_you,'nonce':nonce,'sub_version_num':sub_version_num})
      
      if version < 209:
        return ret
      else:
        start_height = self.helper.uint32()
        ret.update({'start_height':start_height})
    return ret
    
  def parse_verack(self):
    return {}
    
  def parse_addr(self):
    count = self.helper.var_uint()
    addrs = []
    for x in range(count):
      if self.version >= 31402:
        timestamp = self.helper.uint32(4)
        node_addr = self.read_addr()
        addrs.append({'timestamp':timestamp,'node_addr':node_addr})
      else:
        node_addr = self.read_addr()
        addrs.append({'node_addr':node_addr})
    return addrs
    
  def parse_inv(self):
    count = self.helper.var_uint()
    invs = []
    for x in range(count):
      invs.append(self.read_inv_vect())
    return {'invs':invs}
    
  def parse_getblocks(self):
    version = self.helper.uint32()
    count = self.helper.var_uint()
    starts = []
    for x in range(count):
      starts.append(self.helper.read(32))
    stop = self.helper.read(32)
    return {'version':version,'starts':starts,'stop':stop}
    
  def parse_outpoint(self):
    out_hash = self.helper.read(32)
    out_index = self.helper.uint32()
    return {'out_hash':out_hash,'out_index':out_index}

  def parse_txin(self):
    outpoint = self.parse_outpoint()
    script_length = self.helper.var_uint()
    script = self.helper.read(script_length)
    sequence = self.helper.uint32()
    return {'outpoint':outpoint,'script':script,'sequence':sequence}
    
  def parse_txout(self):
    value = self.helper.uint64()
    pk_script_length = self.helper.var_uint()
    pk_script = self.helper.read(pk_script_length)
    return {'value':value,'pk_script':pk_script}
    
  def parse_tx(self):
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
    return {'tx_ins':tx_ins,'tx_outs':tx_outs,'lock_time':lock_time}

  def parse_block(self):
    version = self.helper.uint32()
    prev_block = self.helper.read(32)
    merkle_root = self.helper.read(32)
    timestamp = self.helper.uint32()
    
    """
        //1b012dcd
        CBigNum& SetCompact(unsigned int nCompact)
        {
            unsigned int nSize = nCompact >> 24;//1b (27)
            std::vector<unsigned char> vch(4 + nSize);//31
            vch[3] = nSize;
            if (nSize >= 1) vch[4] = (nCompact >> 16) & 0xff;
            if (nSize >= 2) vch[5] = (nCompact >> 8) & 0xff;
            if (nSize >= 3) vch[6] = (nCompact >> 0) & 0xff;
            BN_mpi2bn(&vch[0], vch.size(), this);
            return *this;
        }
    """
    
    bits = self.helper.uint32()
    nonce = self.helper.read(4)
    tx_count = self.helper.var_uint()
    txs = []
    for x in range(tx_count):
      txs.append(self.parse_tx())
      
    return {'version':version,'prev_block':prev_block,'merkle_root':merkle_root,'timestamp':timestamp,'bits':bits,'nonce':nonce,'txs':txs}
    
  def parse_getaddr(self):
    return {}
    
  def parse_checkorder(self):
    return False
    
  def parse_submitorder(self):
    return False
    
  def parse_reply(self):
    return False
  
  def parse_ping(self):
    return {}
    
  def parse_alert(self):
    message = self.helper.string()
    signature = self.helper.string()
    return {'message':message,'signature':signature}
