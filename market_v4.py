from __future__ import print_function
from multiprocessing import Process
import zmq

import itertools
from lib import *
import Queue
context = zmq.Context()
from traders import REQTrader
from brokers import BrokerWithPool, BrokerWithQueueing
from workers import REQWorker, Auth, DBWorker

class OrderRouter:
	"""
	frontend is ROUTER socket accepting authenticated orders
	backend is PUB socket that sends out orders
	"""

	def run(self):
		frontend = self.context.socket(zmq.ROUTER)
		frontend.bind(AddressManager.get_bind_address(self.frontend_name))
		backend = self.context.socket(zmq.PUB)	
		backend.bind(AddressManager.get_bind_address(self.backend_name))
		
		poller = zmq.Poller()
		poller.register(backend, zmq.POLLIN)
		poller.register(frontend, zmq.POLLIN)
		while True:
			pass








AddressManager.register_endpoint('db_frontend', 'tcp', 'localhost', 5560)
AddressManager.register_endpoint('db_backend', 'tcp', 'localhost', 5561)

AddressManager.register_endpoint('market_frontend', 'tcp', 'localhost', 5562)
AddressManager.register_endpoint('market_backend', 'tcp', 'localhost', 5563)


if __name__ == '__main__':

	# for i in xrange(1): REQTrader('trader', None, 'market_frontend', verbose = True).start()	

	# market_broker = BrokerWithQueueing('market_gateway', 'market_frontend', 'market_backend')
	# market_broker.start()
	
	# for i in xrange(1): Auth('authenticator', 'market_backend', 'db_frontend').start()


	db_broker = BrokerWithQueueing('db_pool', 'db_frontend', 'db_backend')
	db_broker.start()

	for i in xrange(1): DBWorker('db_worker', 'db_backend', None, verbose = True).start()


	# sleep(1)
	

















