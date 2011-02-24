import socket
import struct
import hashlib

class ProtocolHelper:
  magic = b'\xF9\xBE\xB4\xD9'

  def __init__(self,address):
      self.buffer = b''
      self.address = address
      self.socket = socket.socket()
      self.checksum = hashlib.sha256()
  
  def buffered_read(self,length,retry=3):
    try:
      while len(self.buffer) < length:
        self.buffer += self.socket.recv(4096)
      ret = self.buffer[:length]
      self.buffer = self.buffer[length:]
      return ret
    except socket.error as e:
      if retry > 0:
        self.socket.connect(self.address)
        return self.buffered_read(length,retry-1)
      else:
        raise e
  
  def read_var_uint(self):
    first_byte = self.buffered_read(1)[0]
    if first_byte < 0xfd:
      return first_byte
    else:
      length,format = {0xfd: (2, '<H'), 0xfe: (4, '<I'), 0xff: (8, '<L')}[first_byte]
      packed = self.buffered_read(length)
      return struct.unpack(format,packed)[0]
  
  def write_var_uint(self,integer):
    self.socket.send(b'\xff'+struct.pack('<L',integer))
  
  def read_string(self):
    length = self.read_var_uint()
    return self.buffered_read(length)
  
  def write_string(self,string):
    write_var_uint(len(string))
    self.socket.send(string)
    
  def read_message_header(self):
    magic = self.buffered_read(4)
    if magic != self.magic:
      raise Exception("Magic value wrong")
      
    command = self.buffered_read(12).decode("ascii").strip('\x00')
    length = struct.unpack('<I',self.buffered_read(4))[0]
    
    if not (command == "version" or command == "verack"):
      checksum = self.buffered_read(4)
    else:
      checksum = None
    
    parsed_payload = {'version':self.parse_version}[command]()
    
    return (command,parsed_payload)
    
  def buffered_checked_read(self,length):
    buf = self.buffered_read(length)
    self.checksum.update(buf)
    
  def buffered_read_delim(self,delim):
    buf = b''
    while not buf.endswith(delim):
      buf += self.buffered_read(1)
    return buf
    
  def unpack_stream(self,format):
    return struct.unpack(format,self.buffered_read(struct.calcsize(format)))
    
  def parse_version(self):  
    version,services,timestamp = self.unpack_stream('<IQQ')
    addr_me = self.buffered_read(26)
    
    ret = {'version':version,'services':services,'timestamp':timestamp,'addr_me':addr_me}
    if version < 106:
      return ret
    else:
      addr_you = self.buffered_read(26)
      nonce = self.buffered_read(8)
      sub_version_num = self.buffered_read_delim(b'\x00')
      
      ret.update({'addr_you':addr_you,'nonce':nonce,'sub_version_num':sub_version_num})
      
      if version < 209:
        return ret
      else:
        start_height = self.unpack_stream('<I')
        ret.update({'start_height':start_height})
    return ret
    
    
