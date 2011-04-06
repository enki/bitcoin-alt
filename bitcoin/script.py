import struct

opcodes = {80: 'OP_RESERVED',
 97: 'OP_NOP',
 98: 'OP_VER',
 99: 'OP_IF',
 100: 'OP_NOTIF',
 101: 'OP_VERIF',
 102: 'OP_VERNOTIF',
 103: 'OP_ELSE',
 104: 'OP_ENDIF',
 105: 'OP_VERIFY',
 106: 'OP_RETURN',
 107: 'OP_TOALTSTACK',
 108: 'OP_FROMALTSTACK',
 109: 'OP_2DROP',
 110: 'OP_2DUP',
 111: 'OP_3DUP',
 112: 'OP_2OVER',
 113: 'OP_2ROT',
 114: 'OP_2SWAP',
 115: 'OP_IFDUP',
 116: 'OP_DEPTH',
 117: 'OP_DROP',
 118: 'OP_DUP',
 119: 'OP_NIP',
 120: 'OP_OVER',
 121: 'OP_PICK',
 122: 'OP_ROLL',
 123: 'OP_ROT',
 124: 'OP_SWAP',
 125: 'OP_TUCK',
 126: 'OP_CAT',
 127: 'OP_SUBSTR',
 128: 'OP_LEFT',
 129: 'OP_RIGHT',
 130: 'OP_SIZE',
 131: 'OP_INVERT',
 132: 'OP_AND',
 133: 'OP_OR',
 134: 'OP_XOR',
 135: 'OP_EQUAL',
 136: 'OP_EQUALVERIFY',
 137: 'OP_RESERVED1',
 138: 'OP_RESERVED2',
 139: 'OP_1ADD',
 140: 'OP_1SUB',
 141: 'OP_2MUL',
 142: 'OP_2DIV',
 143: 'OP_NEGATE',
 144: 'OP_ABS',
 145: 'OP_NOT',
 146: 'OP_0NOTEQUAL',
 147: 'OP_ADD',
 148: 'OP_SUB',
 149: 'OP_MUL',
 150: 'OP_DIV',
 151: 'OP_MOD',
 152: 'OP_LSHIFT',
 153: 'OP_RSHIFT',
 154: 'OP_BOOLAND',
 155: 'OP_BOOLOR',
 156: 'OP_NUMEQUAL',
 157: 'OP_NUMEQUALVERIFY',
 158: 'OP_NUMNOTEQUAL',
 159: 'OP_LESSTHAN',
 160: 'OP_GREATERTHAN',
 161: 'OP_LESSTHANOREQUAL',
 162: 'OP_GREATERTHANOREQUAL',
 163: 'OP_MIN',
 164: 'OP_MAX',
 165: 'OP_WITHIN',
 166: 'OP_RIPEMD160',
 167: 'OP_SHA1',
 168: 'OP_SHA256',
 169: 'OP_HASH160',
 170: 'OP_HASH256',
 171: 'OP_CODESEPARATOR',
 172: 'OP_CHECKSIG',
 173: 'OP_CHECKSIGVERIFY',
 174: 'OP_CHECKMULTISIG',
 175: 'OP_CHECKMULTISIGVERIFY',
 176: 'OP_NOP1',
 177: 'OP_NOP2',
 178: 'OP_NOP3',
 179: 'OP_NOP4',
 180: 'OP_NOP5',
 181: 'OP_NOP6',
 182: 'OP_NOP7',
 183: 'OP_NOP8',
 184: 'OP_NOP9',
 185: 'OP_NOP10',
 253: 'OP_PUBKEYHASH',
 254: 'OP_PUBKEY',
 255: 'OP_INVALIDOPCODE'}



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
    if opcode == 00:
      parsed.append(('OP_PUSH',0))
    elif opcode >= 1 and opcode <= 75:
      parsed.append(('OP_PUSH',opcode))
    elif opcode == 76:
      length = struct.unpack('>B',parser.read(1))[0]
      parsed.append(('OP_PUSH',parser.read(length)))
    elif opcode == 77:
      length = struct.unpack('>H',parser.read(2))[0]
      parsed.append(('OP_PUSH',parser.read(length)))
    elif opcode == 78:
      length = struct.unpack('>I',parser.read(4))[0]
      parsed.append(('OP_PUSH',parser.read(length)))
    elif opcode == 79:
      parsed.append(('OP_PUSH',-1))
    elif opcode == 81:
      parsed.append(('OP_PUSH',1))
    elif opcode >= 82 and opcode <= 96:
      parsed.append(('OP_PUSH',opcode-80))
    else:
      if opcode in opcodes:
        parsed.append((opcodes[opcode],))
      else:
        raise Exception("Un-recognized opcode "+opcode)
      
