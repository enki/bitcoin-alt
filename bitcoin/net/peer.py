import time
import random
import socket
import queue
import threading

import bitcoin.net.message
import bitcoin.net.payload

class Peer(threading.Thread):
  def __init__(self,address,cb,shutdown,addr_me={'services': 1, 'addr': '::ffff:127.0.0.1', 'port': 8333},my_version=32002,my_services=1):
    super(Peer,self).__init__()
    
    self.address = address
    self.socket = socket.socket(socket.AF_INET6)
    self.socket.settimeout(30)
    
    self.reader = bitcoin.net.message.reader(self.socket)
    self.parser = bitcoin.net.payload.parser()
    
    self.my_nonce = b''
    for x in range(8):
      self.my_nonce += bytes([random.randrange(256)])
    
    self.addr_me = addr_me
    self.addr_you = {'services':1,'addr':address[0],'port':address[1]}
    
    self.my_version = my_version
    self.my_services = my_services
    
    self.cb = cb
    self.slock = threading.Lock()
    self.shutdown = shutdown
    self.daemon = True
    
  def run(self):
    self.socket.connect(self.address)
    self.send_version()
    while True:
      try:
        command,raw_payload = self.reader.read()
      except socket.timeout as e:
        print("sending ping")
        self.send_ping()
        continue
      except socket.error as e:
        return
      finally:
        if self.shutdown.is_set():
          return

      p = self.parser.parse(command,raw_payload)
      
      if command == 'version':
        self.handle_version(p)
      elif command in ['verack','ping']:
        pass
      elif self.version:
        self.cb.put({'peer':self,'command':command,'payload':p})
      else:
        raise Exception("received packet before version")
    
  def send_version(self):
    with self.slock:
      p = bitcoin.net.payload.version(self.my_version,self.my_services,int(time.time()),self.addr_me,self.addr_you,self.my_nonce,'',110879)
      bitcoin.net.message.send(self.socket,b'version',p)
    
  def send_verack(self):
    with self.slock:
      bitcoin.net.message.send(self.socket,b'verack',b'')
    
  def send_inv(self,invs):
    with self.slock:
      p = bitcoin.net.payload.inv(invs,self.version)
      bitcoin.net.message.send(self.socket,b'inv',p)
    
  def send_getaddr(self):
    with self.slock:
      bitcoin.net.message.send(self.socket,b'getaddr',b'')
    
  def send_getdata(self,invs):
    with self.slock:
      p = bitcoin.net.payload.getdata(invs,self.version)
      bitcoin.net.message.send(self.socket,b'getdata',p)
    
  def send_ping(self):
    with self.slock:
      bitcoin.net.message.send(self.socket,b'ping',b'')
  
  def handle_version(self,p):
    self.version = p['version']
    self.nonce = p['nonce']
    self.services = p['services']
    self.send_verack()
