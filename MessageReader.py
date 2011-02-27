class MessageReaders:
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
  
  def get_message(self):
    reader = StreamReader.StreamReader(self.socket)
    magic = stream.read(4)
    if magic != self.magic:
      raise Exception("Magic value wrong")
    
    command = stream.fixed_string(12)
    length = stream.uint32()
    
    if command == "version" or command == "verack":
      checksum = None
    else:
      checksum = stream.read(4)
    
    if length > 100*1000:#100 KB
      raise Exception("Packet is too large")
      
    payload = stream.read(length)
    
    return (command,payload,checksum)
    
    
    
