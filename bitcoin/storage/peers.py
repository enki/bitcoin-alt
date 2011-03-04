import threading
import random

class peers:
  def __init__(self):
    self.peers = set()
    self.plock = threading.Lock()
  
  def add(self,address):
    with self.plock:
      if address not in self.peers:
        self.peers.add(address)
  
  def get(self,count):
    with self.plock:
      random.choice(self.peers)
