import socket
import struct
import hashlib

class StreamHelper:
  def __init__(self,sock):
    self.buffer = b''
    self.socket = sock
    self.checksum = None
    
  def buffered_read(self,length):
    while len(self.buffer) < length:
      self.buffer += self.socket.recv(4096)
    ret = self.buffer[:length]
    self.buffer = self.buffer[length:]
    if self.checksum:
      self.checksum.update(ret)
    return ret
    
  def buffered_read_delim(self,delim):
    buf = b''
    while not buf.endswith(delim):
      buf += self.buffered_read(1)
    return buf
    
  def unpack_stream(self,format):
    return struct.unpack(format,self.buffered_read(struct.calcsize(format)))

  def read_uint16(self,little_endian=True):
    if little_endian:
      format = '<H'
    else:
      format = '>H'
    return self.unpack_stream(format)[0]
    
  def write_uint16(self,integer,little_endian=True):
    if little_endian:
      format = '<H'
    else:
      format = '>H'
    self.socket.sendall(struct.pack(format,integer))
    
  def read_uint32(self,little_endian=True):
    if little_endian:
      format = '<I'
    else:
      format = '>I'
    return self.unpack_stream(format)[0]
    
  def write_uint32(self,integer,little_endian=True):
    if little_endian:
      format = '<I'
    else:
      format = '>I'
    self.socket.sendall(struct.pack(format,integer))
    
  def read_uint64(self,little_endian=True):
    if little_endian:
      format = '<Q'
    else:
      format = '>Q'
    return self.unpack_stream(format)[0]
    
  def write_uint64(self,integer,little_endian=True):
    if little_endian:
      format = '<Q'
    else:
      format = '>Q'
    self.socket.sendall(struct.pack(format,integer))
    
  def read_var_uint(self):
    first_byte = self.buffered_read(1)[0]
    if first_byte < 0xfd:
      return first_byte
    else:
      length,format = {0xfd: (2, '<H'), 0xfe: (4, '<I'), 0xff: (8, '<L')}[first_byte]
      packed = self.buffered_read(length)
      return struct.unpack(format,packed)[0]
  
  def write_var_uint(self,integer):
    self.socket.sendall(b'\xff'+struct.pack('<L',integer))
  
  def read_string(self):
    length = self.read_var_uint()
    return self.buffered_read(length).decode('ascii')
    
  def read_null_string(self):
    return self.buffered_read_delim(b'\x00').decode('ascii').strip('\x00')
    
  def read_fixed_string(self,length):
    return self.buffered_read(length).decode('ascii').strip('\x00')
  
  def write_string(self,string):
    write_var_uint(len(string))
    self.socket.sendall(string)
    
  def write_null_string(self,string):
    self.socket.sendall(string+'\x00')
    
  def write_fixed_string(self,string,length):
    if len(string) > length:
      raise Exception("string is too long")
    self.socket.sendall(string.encode('ascii'))
    self.socket.sendall(b"\x00"*(length-len(string.encode('ascii'))))
    
  def start_checksum(self):
    self.checksum = hashlib.sha256()
    
  def check_checksum(self,checksum):
    ret = hashlib.sha256(self.checksum.digest())[:4] == checksum
    self.checksum = None
    return ret
