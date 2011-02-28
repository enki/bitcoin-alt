import hashlib
import struct

magic = b'\xF9\xBE\xB4\xD9'

class reader:
  def __init__(self,s):
    self.buffer = b''
    self.socket = s
  
  def buffered_read(self,length):
    while len(self.buffer) < length:
      self.buffer += self.socket.recv(4096)
    ret = self.buffer[:length]
    self.buffer = self.buffer[length:]
    return ret
  
  def read(self):
    m = self.buffered_read(4)
    if m != magic:
      raise Exception("Magic value wrong")
    
    command = self.buffered_read(12).decode('ascii').strip('\x00')
    length = struct.unpack('<I',self.buffered_read(struct.calcsize('<I')))[0]
    
    if command == "version" or command == "verack":
      checksum = None
    else:
      checksum = self.buffered_read(4)
      length -= len(checksum)
    
    if length > 100*1000:#100 KB
      raise Exception("Packet is too large")
    
    payload = self.buffered_read(length)
    
    import pdb
    pdb.set_trace()
    
    if checksum:
      if hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4] != checksum:
        raise Exception("checksum failure")
    
    return (command,payload)
    
def send(s,command,payload):
  s.sendall(magic)
  if len(command) > 12:
    raise Exception("command too long")
  s.sendall(command)
  s.sendall(b'\x00'*(12-len(command)))
  s.sendall(struct.pack('<I',len(payload)))
  if not (command == "version" or command == "verack"):
    s.sendall(hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4])
  s.sendall(payload)

    
    
    
