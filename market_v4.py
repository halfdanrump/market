from __future__ import print_function
from multiprocessing import Process
import zmq

import itertools
from lib import *
import Queue
context = zmq.Context()
from traders import Trader
from brokers import JobQueueBroker
from workers import Auth, DBWorker
from time import sleep


AddressManager.register_endpoint('db_frontend', 'tcp', 'localhost', 5560)
AddressManager.register_endpoint('db_backend', 'tcp', 'localhost', 5561)

AddressManager.register_endpoint('market_frontend', 'tcp', 'localhost', 5562)
AddressManager.register_endpoint('market_backend', 'tcp', 'localhost', 5563)



def run_auth_cluster(n_workers, verbose = False):
	market_broker = JobQueueBroker('market_gateway', 'market_frontend', 'market_backend', verbose = verbose)
	market_broker.start()
	for i in xrange(n_workers): Auth('authenticator', 'market_backend', 'db_frontend', verbose = verbose).start()



def run_db_cluster(n_workers, verbose = False):
	db_broker = JobQueueBroker('db_broker', 'db_frontend', 'db_backend', verbose = verbose)
	db_broker.start()
	for i in xrange(n_workers): DBWorker('db_worker', 'db_backend', None, verbose = verbose).start()


# def run_auction_cluster(n_workers, verbose = False):
# 	auction_broker = JobQueueBroker('db_broker', 'db_frontend', 'db_backend', verbose = verbose)
# 	db_broker.start()
# 	for i in xrange(n_workers): Auction('auction', '', None, verbose = verbose).start()



if __name__ == '__main__':
	Trader('trader', None, 'market_frontend', verbose = True).start()	
	run_auth_cluster(1, False)	
	run_db_cluster(1, False)













