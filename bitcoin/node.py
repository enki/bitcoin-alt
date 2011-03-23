

  def connect_blocks(self,peer):    
    try:
      self.storage.connect_blocks()
      heads = self.storage.get_heads()
      tails = self.storage.get_tails()
      try:
        peer.send_getblocks(heads)
        for tail in tails:
          peer.send_getblocks(heads,tail)
      except AttributeError:
        pass#this is raised when no version has yet been received
    except sqlite3.OperationalError:
      pass
      
  def handle_connect(self,peer,payload):
    self.connect_blocks(peer)
    pass
      
  def handle_addr(self,peer,payload):
    for addr in payload['addrs']:
      self.peers.add((addr['node_addr']['addr'],addr['node_addr']['port']))
  
  def handle_inv(self,peer,payload):
    invs = []
    for inv in payload['invs']:
      if inv['type'] == 1:
        if not self.storage.get_tx(inv['hash']):
          invs.append(inv)
      if inv['type'] == 2:
        if not self.storage.get_block(inv['hash']):
          invs.append(inv)
    peer.send_getdata(invs)
    
  def handle_getdata(self,peer,payload):
    for inv in payload['invs']:
      if inv['type'] == 1:
        d = self.storage.get_tx(inv['hash'])
        if d:
          peer.send_tx(d)
      if inv['type'] == 2:
        d = self.storage.get_block(inv['hash'])
        if d:
          peer.send_block(d)
  
  def handle_getblocks(self,peer,payload):
    pass
    
  def handle_getheaders(self,peer,payload):
    pass
    
  def handle_tx(self,peer,payload):
    self.storage.put_tx(payload)
    
  def handle_block(self,peer,payload):
    self.storage.put_block(payload)
    
  def handle_headers(self,peer,payload):
    pass
    
  def handle_getaddr(self,peer,payload):
    pass
    
  def handle_checkorder(self,peer,payload):
    pass
    
  def handle_submitorder(self,peer,payload):
    pass
    
  def handle_reply(self,peer,payload):
    pass
    
  def handle_alert(self,peer,payload):
    pass


