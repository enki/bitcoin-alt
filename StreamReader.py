import socket
import struct
import hashlib

class StreamReader:
  def __init__(self,sock):
    self.buffer = b''
    self.socket = sock
    
  def read(self,length):
    while len(self.buffer) < length:
      self.buffer += self.socket.recv(4096)
    ret = self.buffer[:length]
    self.buffer = self.buffer[length:]
    return ret
    
  def read_delim(self,delim):
    buf = b''
    while not buf.endswith(delim):
      buf += self.read(1)
    return buf
    
  def unpack(self,format):
    return struct.unpack(format,self.read(struct.calcsize(format)))

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
    first_byte = self.read(1)[0]
    if first_byte < 0xfd:
      return struct.unpack('<B',first_byte)[0]
    else:
      format = {0xfd: '<H', 0xfe: '<I', 0xff: '<L'}[first_byte]
      return self.unpack(format)[0]
  
  def string(self):
    length = self.read_var_uint()
    return self.buffered_read(length).decode('ascii')
    
  def null_string(self):
    return self.buffered_read_delim(b'\x00').decode('ascii').strip('\x00')
    
  def fixed_string(self,length):
    return self.buffered_read(length).decode('ascii').strip('\x00')
