import socket
import struct
import hashlib

class ProtocolHelper:
  magic = b'\xF9\xBE\xB4\xD9'

  def __init__(self,address):
      self.buffer = b''
      self.address = address
      self.socket = socket.socket()
  
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
      
    payload = self.buffered_read(length)
    
    if checksum:
      #TODO verify this works
      if not hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4] == checksum:
        raise Exception("checksum mismatch")
    
    parsed_payload = {'version':self.parse_version}[command](payload)
    
    return (command,parsed_payload)
    
  def parse_version(self,payload):
    version,services,timestamp = struct.unpack('<IQQ',payload[:struct.calcsize('<IQQ')])
    payload = payload[struct.calcsize('<IQQ'):]
    
    addr_me = addr_you = payload[:26]
    payload = payload[26:]
    
    if version < 106:
      return {'version':version,'services':services,'timestamp':timestamp,'addr_me':addr_me}
    else:
      addr_you = payload[:26]
      payload = payload[26:]
      
      nonce = payload[:8]
      payload = payload[8:]
      
    
    
