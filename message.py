import hashlib
import struct

class reader:
  magic = b'\xF9\xBE\xB4\xD9'
  
  def __init__(self,s):
    self.buffer = b''
    self.socket = s
  
  def buffered_read(self,length):
    while len(self.buffer) < length:
      self.buffer += self.socket.recv(4096)
    ret = self.buffer[:length]
    self.buffer = self.buffer[length:]
    return ret
  
  def get(self):
    magic = self.buffered_read(4)
    if magic != self.magic:
      raise Exception("Magic value wrong")
    
    command = self.buffered_read(12).decode('ascii').strip('\x00')
    length = struct.unpack('<I',self.buffered_read(struct.calcsize('<I')))[0]
    
    if command == "version" or command == "verack":
      checksum = None
    else:
      checksum = self.buffered_read(4)
    
    if length > 100*1000:#100 KB
      raise Exception("Packet is too large")
      
    payload = self.buffered_read(length)
    
    if checksum:
      if hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4] != checksum:
        raise Exception("checksum failure")
    
    return (command,payload)
    
    
    
