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
    self.uint64(services)
    self.write(addr)
    self.uint16(port,little_endian=False)
    
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
  
  def parse(self,command,payload):
    try:
      self.helper = buffer_parser(payload)
      return {
      'version':self.parse_version,
      'verack':self.parse_verack,
      'addr':self.parse_addr,
      'inv':self.parse_inv,
      'getdata':self.parse_inv,
      'getblocks':self.parse_getblocks,
      'getheaders':self.parse_getblocks,
      'tx':self.parse_tx,
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
    
    addr_me = self.helper.read(26)
    
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
    return (outpoint,script,sequence)
    
  def parse_txout(self):
    value = self.helper.uint64()
    pk_script_length = self.helper.var_uint()
    pk_script = self.helper.read(pk_script_length)
    return (value,pk_script)
    
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
    
