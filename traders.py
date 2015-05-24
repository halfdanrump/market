from lib import *
import itertools
import atexit
import sys



class MultiDict(multi_key_dict):
	def __repr__(self):
		return str(self.items())

	def __setitem__(self, key, value):
		if self.get_other_keys(key):
			raise KeyError('Key {} is ')

# class Sorck(zmq.Socket):

# 	def __init__(self, context, socket_type, name, endpoint, bind, handler):
# 		super(Sorck, self).__init__(context = zmq.Context(), socket_type = 5)
# 		assert isinstance(name, str)
# 		self.name = name
# 		assert isinstance(name, str)
# 		self.endpoint = endpoint
# 		assert isinstance(socktype, int)
# 		self.socktype = socktype
# 		assert isinstance(bind, bool)
# 		self.bind = bind
# 		assert callable(handler) or handler == None
# 		self.handler = handler
# 		print(socket_type)
		
		



	# def __repr__(self):
	# 	return str(self.sockets)

context = zmq.Context()

class Trader(AgentProcess):
	"""
	no frontend
	backend is REQ or DEALER socket sending orders
	"""

	__sockets__ = [
		Sock(**{	'name' : 'backend', 
					'socket_type' : zmq.DEALER,
					'bind': False,
					'handler' : 'backend_handler'})
		]

	# for socket in sockets:
	# 	pass
	# sockets = [{'name' : 'backend', 
	# 			'endpoint' : 'tcp://localhost:5000',
	# 			'type' : zmq.DEALER,
	# 			'bind': False,
	# 			'recv_handler' : None
	# 			}]

	# def __init__(self, name, endpoints, verbose):
	# 	assert set(self.sockets.names()) == set(endpoints.keys()), "Argument 'endpoints' must be a dictionary with endpoint names as items for each of the keys {}".format(self.sockets.names())
	# 	for socket_name, endpoint in endpoints.items():
	# 		self.init_socket(socket_name)

	
	# def poll_sockets(self):
	# 	for socket in dict(self.poller.poll()):
	# 		self.sockets[socket].handler()

	def setup(self):
		# self.new_socket(endpoint, socket_name, socket_type, bind, handler)
		# self.backend = self.context.socket(zmq.DEALER)
		# self.backend.connect(AddressManager.get_connect_address(self.backend_name))

		# self.poller = zmq.Poller()
		# self.poller.register(self.backend, zmq.POLLIN)
		self.say('ready')
		self.n_replies = 0

	def backend_handler(self):
		p = self.backend.recv()
		print(p)

	def iteration(self):
		order = 'new order {}'.format(random())
		package = Package(msg = order)
		self.say('Sending on backend: {}'.format(package))
		# package.send(self.backend)
		# print(self.backend.socket_type)
		# context = zmq.Context()
		# self.backend = context.socket(zmq.DEALER)
		# print(self.backend)
		print(self.context)
		print(self.backend.context)
		self.backend.send_multipart(["", "hello"])
		# self.backend.send('asd')
		# package.send(self.backend)
		# ack = self.backend.recv_multipart()
		# self.n_replies += 1
		# self.say('From backend: {}'.format(ack))
		# sleep(1)
		# if self.n_replies > 1: sys.exit(0)

