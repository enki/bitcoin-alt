#!/usr/bin/env python3

import logging
import sys
import threading
import time
import os
import socket

import bitcoin.peers

os.nice(5)

static_peers = [('::ffff:'+sockaddr[0],sockaddr[1]) for family,socktype,proto,canonname,sockaddr in socket.getaddrinfo("bitseed.bitcoin.org.uk",8333,socket.AF_INET)]
try:
  static_peers.extend([sockaddr for family,socktype,proto,canonname,sockaddr in socket.getaddrinfo("bitseed.bitcoin.org.uk",8333,socket.AF_INET6)])
except socket.gaierror as e:
  if e.errno == -2:
    pass
  else:
    raise e

logging.basicConfig()
logger = logging.getLogger('')
if '-d' in sys.argv[1:]:
  logger.setLevel(logging.INFO)
if '-dd' in sys.argv[1:]:
  logger.setLevel(logging.DEBUG)

shutdown = threading.Event()
shutdown.clear()

storage = bitcoin.storage.Storage()

peers = bitcoin.peers.Peers(shutdown,storage,1)
peers.start()

for peer in static_peers:
  logger.info('Adding peer %s', peer)
  peers.add(peer)

while True:
  try:
    time.sleep(0.1)
  except KeyboardInterrupt:
    logger.info('Sending the shutdown event')
    shutdown.set()
    peers.join()
    sys.exit(0)
