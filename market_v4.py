from __future__ import print_function
from multiprocessing import Process
import zmq

import itertools
from lib import *
import Queue
context = zmq.Context()
from traders import REQTrader
from brokers import BrokerWithPool, BrokerWithQueueing
from workers import REQWorkerThread

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




class Auth(AgentProcess):
	"""
	REQ worker is frontend, connected to a broker
	backend is DEALER socket, handing jobs to DB broker

	"""
	
	def check_order(self, order):
		sleep(random())
		return True

	def run(self):
		frontend = self.context.socket(zmq.REQ)
		frontend.connect(AddressManager.get_connect_address(self.frontend_name))
		backend = self.context.socket(zmq.DEALER)
		backend.connect(AddressManager.get_connect_address(self.backend_name))

		poller = zmq.Poller()
		poller.register(frontend, zmq.POLLIN)
		poller.register(backend, zmq.POLLIN)

		self.say('ready')

		frontend.send_multipart(WorkerMsg(STATUS_READY).message)

		pending = dict()

		while True:

			sockets = dict(poller.poll(100))
			if frontend in sockets:
				msg = frontend.recv_multipart()
				
				trader_id, order = msg[0], msg[2]
				self.say('message from trader: {}'.format(trader_id))
				if self.check_order(order):
					### Send ACK back to trader
					msg = WorkerMsg(STATUS_READY, trader_id, ORDER_RECEIVED)
					### Send order to database
					backend.send_multipart([trader_id, "", order])
				else:
					msg = WorkerMsg(STATUS_READY, trader_id, INVALID_ORDER)
				# frontend.send_multipart([trader_id, "", "ASD"])
				frontend.send_multipart(msg.message)

			if backend in sockets:
				self.say(backend.recv_multipart())



AddressManager.register_endpoint('db_frontend', 'tcp', 'localhost', 5560)
AddressManager.register_endpoint('db_backend', 'tcp', 'localhost', 5561)

AddressManager.register_endpoint('market_frontend', 'tcp', 'localhost', 5562)
AddressManager.register_endpoint('market_backend', 'tcp', 'localhost', 5563)


if __name__ == '__main__':
	
	for i in xrange(1): REQTrader('trader', None, 'market_frontend').start()	
	# for i in xrange(1): REQTrader('trader', zmqSocket(type = )).start()	
	
	market_broker = BrokerWithQueueing('market_gateway', 'market_frontend', 'market_backend')
	market_broker.start()
	
	for i in xrange(1): Auth('authenticator', 'market_backend', 'db_frontend').start()


	# db_broker = BrokerWithPool('db_pool', 'db_frontend', 'db_backend')
	# db_broker.start()

















