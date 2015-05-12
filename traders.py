from lib import *
import itertools
import atexit


class Trader(AgentProcess):
	"""
	no frontend
	backend is REQ or DEALER socket sending orders
	"""

	def setup(self):
		self.backend = self.context.socket(zmq.DEALER)
		self.backend.connect(AddressManager.get_connect_address(self.backend_name))

		self.poller = zmq.Poller()
		self.poller.register(self.backend, zmq.POLLIN)
		self.say('ready')
		self.n_replies = 0

	def iteration(self):
		order = 'new order {}'.format(random())
		package = Package(msg = order)
		self.say('Sending on backend: {}'.format(package))
		package.send(self.backend)
		ack = self.backend.recv_multipart()
		self.n_replies += 1
		self.say('From backend: {}'.format(ack))
		sleep(1)