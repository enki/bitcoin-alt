import struct

class script_parser:
  def __init__(self,script):
    self.script = script
    self.offset = 0
  
  def read(self,length):
    ret = self.script[self.offset:]
    self.offset += length
    return ret
  
  def opcode(self):
    return self.read(1)
    
def parse(script):
  parser = script_parser(script)
  opcode = parser.opcode()
  parsed = []
  while opcode:
    if opcode == 0x00:
      parsed.append(('OP_PUSH',0))
    elif opcode >= 1 and opcode <= 75:
      parsed.append(('OP_PUSH',opcode))
    elif opcode == 0x76:
      length = struct.unpack('>B',parser.read(1))[0]
      parsed.append(('OP_PUSH',parser.read(length)))
    elif opcode == 0x77:
      length = struct.unpack('>H',parser.read(2))[0]
      parsed.append(('OP_PUSH',parser.read(length)))
    elif opcode == 0x78:
      length = struct.unpack('>I',parser.read(4))[0]
      parsed.append(('OP_PUSH',parser.read(length)))
    elif opcode == 0x79:
      parsed.append(('OP_PUSH',-1))
    elif opcode == 0x81:
      parsed.append(('OP_PUSH',1))
    elif opcode >= 0x82 and opcode <= 0x96:
      parsed.append(('OP_PUSH',opcode-0x80))
    else:
      raise Exception("Un-recognized opcode "+opcode)
      
