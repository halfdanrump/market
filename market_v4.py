from __future__ import print_function
from multiprocessing import Process
import zmq

import itertools
from lib import *
import Queue
context = zmq.Context()
from traders import Trader
from brokers import PingPongBroker
from workers import REQWorker, Auth, DBWorker
from time import sleep
import sys

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






def run_auth_cluster(n_workers, verbose = False):
	market_broker = PingPongBroker('market_gateway', 'market_frontend', 'market_backend', verbose = verbose)
	market_broker.start()
	for i in xrange(n_workers): Auth('authenticator', 'market_backend', 'db_frontend', verbose = verbose).start()



def run_db_cluster(n_workers, verbose = False):
	db_broker = PingPongBroker('db_broker', 'db_frontend', 'db_backend', verbose = verbose)
	db_broker.start()
	for i in xrange(n_workers): DBWorker('db_worker', 'db_backend', None, verbose = verbose).start()


if __name__ == '__main__':

	Trader('trader', None, 'market_frontend', verbose = False).start()	
	run_auth_cluster(1, False)	
	run_db_cluster(1, True)
	# for i in xrange(1): REQTrader('trader', None, 'market_frontend', verbose = True).start()	

	# market_broker = BrokerWithQueueing('market_gateway', 'market_frontend', 'market_backend', verbose = True)
	# market_broker.start()
	
	
	# for i in xrange(1): Auth('authenticator', 'market_backend', 'db_frontend', verbose =False).start()




	# db_broker = BrokerWithQueueing('db_pool', 'db_frontend', 'db_backend', verbose = False)
	# db_broker.start()

	# for i in xrange(1): DBWorker('db_worker', 'db_backend', None, verbose = False).start()


	# sleep(1)
	

















