class parser:
  def __init__(self,script):
    self.buffer = script
    self.script = script
    self.parsed = []
  
  def read_opcode(self):
    opcode = self.buffer[0]
    self.buffer = self.buffer[1:]
    return opcode
  
  def parse(self):
    while True:
      opcode = self.read_opcode()
      if opcode == 0x00:
        self.parsed.append()
