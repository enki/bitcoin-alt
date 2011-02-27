import socket
import struct
import hashlib
from MessageReader import MessageReader
from StreamReader import StreamReader

from StreamHelper import StreamHelper

class ProtocolHelper:
  magic = b'\xF9\xBE\xB4\xD9'
  my_version = 32002
  services = 1

  def __init__(self,address):
      self.address = address
      self.socket = socket.socket(socket.AF_INET6)
      self.socket.connect(address)
      self.stream = StreamHelper(self.socket)
    
  def read_message(self):
    magic = self.stream.buffered_read(4)
    if magic != self.magic:
      raise Exception("Magic value wrong")
    
    command = self.stream.read_fixed_string(12)
    length = self.stream.read_uint32()
    
    if not (command == "version" or command == "verack"):
      self.checksum = self.stream.buffered_read(4)
    else:
      self.checksum = None
    
    try:
      parsed_payload = {'version':self.parse_version,
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
    
    return (command,parsed_payload)
    
  def send_message(self,command,payload,magic=b'\xF9\xBE\xB4\xD9',length=None,checksum=None):
    self.stream.socket.sendall(magic)
    self.stream.write_fixed_string(command,12)
    if not length:
      length = len(payload)
    self.stream.write_uint32(length)
    if not (command == "version" or command == "verack"):
      if not checksum:
        checksum = hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4]
      self.stream.socket.sendall(checksum)
    self.stream.socket.sendall(payload)
    
  def read_addr(self):
    services = self.stream.read_uint64()
    addr = self.stream.buffered_read(16)
    port = self.stream.read_uint16(False)
    return (services,addr,port)
    
  def pack_addr(self,services,addr,port):
    packed = struct.pack('<Q',services)
    packed += addr
    packed += struct.pack('>H',port)
    return packed
    
  def read_inv_vect(self):
    inv_type = self.stream.read_uint32()
    inv_hash = self.stream.buffered_read(32)
    return (inv_type,inv_hash)
  
  def pack_inv_vect(self,inv_type,inv_hash):
    packed = struct.pack('<I',inv_type)
    packed += inv_hash
    return packed
    
  def parse_version(self):
    version = self.stream.read_uint32()
    services = self.stream.read_uint64()
    timestamp = self.stream.read_uint64()
    
    self.version = version
    
    addr_me = self.stream.buffered_read(26)
    
    ret = {'version':version,'services':services,'timestamp':timestamp,'addr_me':addr_me}
    if version < 106:
      return ret
    else:
      addr_you = self.read_addr()
      nonce = self.stream.buffered_read(8)
      sub_version_num = self.stream.read_null_string()
      
      ret.update({'addr_you':addr_you,'nonce':nonce,'sub_version_num':sub_version_num})
      
      if version < 209:
        return ret
      else:
        start_height = self.stream.read_uint32()
        ret.update({'start_height':start_height})
    return ret
    
  def send_version(self,version,services,timestamp,addr_me,addr_you=None,nonce=None,sub_version_num=None,start_height=None):
    payload = b''
    payload = struct.pack('<IQQ',version,services,timestamp)
    
    payload += self.pack_addr(*addr_me)
    
    if version < 106:
      self.send_message('version',payload)
    else:
      payload += self.pack_addr(*addr_you)
      payload += nonce
      payload += sub_version_num.encode('ascii') + b'\x00'
      
      if version < 209:
        self.send_message('version',payload)
      else:
        payload += struct.pack('<I',start_height)
        self.send_message('version',payload)
    
  def parse_verack(self):
    return {}
    
  def send_verack(self):
    self.send_message('verack',b'')
    
  def parse_addr(self):
    self.stream.start_checksum()
    count = self.stream.read_var_uint()
    addrs = []
    for x in range(count):
      if self.version >= 31402:
        timestamp = self.stream.read_uint32(4)
        node_addr = self.read_addr()
        addrs.append({'timestamp':timestamp,'node_addr':node_addr})
      else:
        node_addr = self.read_addr()
        addrs.append({'node_addr':node_addr})
    if not self.stream.check_checksum(self.checksum):
      raise Exception("checksum failed")
    return addrs
    
  def send_addr(self,addrs):
    payload = b''
    payload += b'\xff'+struct.pack('<Q',len(addrs))
    if self.version >= 31402:
      payload += struct.pack('<I',int(time.time()))
    for addr in addrs:
      payload += pack_addr(addr)
    self.send_message('addr',payload)
    
  def parse_inv(self):
    self.stream.start_checksum()
    count = self.stream.read_var_uint()
    invs = []
    for x in range(count):
      invs.append(self.read_inv_vect())
    if not self.stream.check_checksum(self.checksum):
      raise Exception("checksum failed")
    return {'invs':invs}
    
  def send_inv(self,invs):
    payload = b''
    payload += b'\xff'+struct.pack('<Q',len(invs))
    for inv in invs:
      payload += pack_inv_vect(inv)
    self.send_message('inv',payload)
    
  def send_getdata(self,invs):
    payload = b''
    payload += b'\xff'+struct.pack('<Q',len(invs))
    for inv in invs:
      payload += self.pack_inv_vect(*inv)
    self.send_message('getdata',payload)
    
  def parse_getblocks(self):
    self.stream.start_checksum()
    version = self.stream.read_uint32()
    count = self.stream.read_var_uint()
    starts = []
    for x in range(count):
      starts.append(self.stream.buffered_read(32))
    stop = self.stream.buffered_read(32)
    if not self.stream.check_checksum(self.checksum):
      raise Exception("checksum failed")
    return {'version':version,'starts':starts,'stop':stop}
    
  def send_getblocks(self,starts,stop):
    payload = b''
    payload += struct.pack('<I',self.my_version)
    payload += b'\xff'+struct.pack('<Q',len(starts))
    for start in starts:
      payload += start
    payload += stop
    self.send_message('getblocks',payload)
    
  def send_getheaders(self,starts,stop):
    payload = b''
    payload += struct.pack('<I',self.my_version)
    payload += b'\xff'+struct.pack('<Q',len(starts))
    for start in starts:
      payload += start
    payload += stop
    self.send_message('getheaders',payload)
    
  def parse_outpoint(self):
    out_hash = self.stream.buffered_read(32)
    out_index = self.stream.read_uint32()
    return {'out_hash':out_hash,'out_index':out_index}
    
  def pack_outpoint(self,out_hash,out_index):
    payload = b''
    payload += out_hash
    payload += struct.pack('<I',out_index)
    return payload

  def parse_txin(self):
    outpoint = self.parse_outpoint()
    script_length = self.stream.read_var_uint()
    script = self.stream.buffered_read(script_length)
    sequence = self.stream.read_uint32()
    return (outpoint,script,sequence)
    
  def pack_txin(self,outpoint,script,sequence):
    payload = b''
    payload += self.pack_outpoint(*outpoint)
    payload += b'\xff'+struct.pack('<Q',len(script))
    payload += script
    payload += struct.pack('<I',sequence)
    return payload
    
  def parse_txout(self):
    value = self.stream.read_uint64()
    pk_script_length = self.stream.read_var_uint()
    pk_script = self.stream.buffered_read(pk_script_length)
    return (value,pk_script)
    
  def pack_txout(self,value,pk_script):
    payload = b''
    payload += struct.pack('<Q',value)
    payload += b'\xff'+struct.pack('<Q',len(pk_script))
    payload += pk_script
    return payload
    
  def parse_tx(self):
    self.stream.start_checksum()
    version = self.stream.read_uint32()
    tx_in_count = self.stream.read_var_uint()
    tx_ins = []
    for x in range(tx_in_count):
      tx_ins.append(self.parse_txin())
    tx_out_count = self.stream.read_var_uint()
    tx_outs = []
    for x in range(tx_out_count):
      tx_outs.append(self.parse_txout())
    lock_time = self.stream.read_uint32()
    if not self.stream.check_checksum(self.checksum):
      raise Exception("checksum failed")
    return {'tx_ins':tx_ins,'tx_outs':tx_outs,'lock_time':lock_time}
    
  def send_tx(self,version,tx_ins,tx_outs,lock_time):
    payload = b''
    #payload += self.pack_txin(
    self.send_message('tx',payload)
    
  def parse_getaddr(self):
    return {}
    
  def parse_block(self):
    return {}
