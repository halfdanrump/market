from lib import *
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
		while True:
			sockets = dict(poller.poll(1))
			if backend in sockets:
				ack = backend.recv_multipart()
				self.say(ack)
			else:
				order = 'new order {}'.format(random())
				# msg = ClientMsg(order, backend.TYPE)
				# self.say(order)
				backend.send_multipart(["", order])
				sleep(1)
