from lib import *
import itertools

class REQTrader(AgentProcess):
	"""
	no frontend
	backend is REQ or DEALER socket sending orders
	"""

	def run(self):
		backend = self.context.socket(zmq.DEALER)
		backend.connect(AddressManager.get_connect_address(self.backend_name))
		poller = zmq.Poller()
		poller.register(backend, zmq.POLLIN)
		self.say('ready')
		n_orders = 0
		while True:
			sockets = dict(poller.poll(1))
			if backend in sockets:
				ack = backend.recv_multipart()
				self.say('From backend: {}'.format(ack))
			if n_orders < 1:
				order = 'new order {}'.format(random())
				# msg = ClientMsg(order, backend.TYPE)
				self.say(order)
				package = Package(msg = order)
				package.send(backend)
				# backend.send_multipart(["", order])
				n_orders += 1
				sleep(1)
