from lib import *
import itertools
import atexit

class Trader(AgentProcess):
	"""
	no frontend
	backend is REQ or DEALER socket sending orders
	"""



	def run(self):
		sleep(0.1)
		n_orders = 10000

		backend = self.context.socket(zmq.DEALER)
		backend.connect(AddressManager.get_connect_address(self.backend_name))
		poller = zmq.Poller()
		poller.register(backend, zmq.POLLIN)
		self.say('ready')
		n_replies = 0
		
		for i in range(n_orders):
			order = 'new order {}'.format(random())
			package = Package(msg = order)
			self.say('Sending on backend: {}'.format(package))
			package.send(backend)

		while n_replies < n_orders:
			sockets = dict(poller.poll(1))
			if backend in sockets:
				ack = backend.recv_multipart()
				n_replies += 1
				self.say('From backend: {}'.format(ack))
			
		print('DONE')
