from __future__ import print_function
from multiprocessing import Process
from threading import Thread, Event
import abc
from redis import Redis
import zmq
from time import sleep
from random import random
from datetime import datetime
from collections import namedtuple
from copy import copy
from collections import deque
import re
class MsgCode:
	STATUS_READY = "READY"
	ORDER_RECEIVED = "ORDER VERIFIED"
	ORDER_STORED = "ORDER_STORED"
	JOB_COMPLETE = "JOB COMPLETE"
	INVALID_ORDER = "INVALID ORDER"
	PING = "PING"
	PONG = "PONG"
	DISCONNECT = "DISCONNECT"



STATUS_READY = "READY"
ORDER_RECEIVED = "ORDER VERIFIED"
JOB_COMPLETE = "JOB COMPLETE"
INVALID_ORDER = "INVALID ORDER"

# Package = namedtuple('Package', 'addr msg encapsulated')



class DQueue(deque):
	"""
	Just a convenience class to mimic the interface of Queue.Queue
	"""
	def __init__(self, item_type = None):
		self.item_type = item_type

	def put(self, item):
		if self.item_type:
			assert isinstance(item, self.item_type), 'Expected item of type {} but got {}'.format(self.item_type.__class__im, type(item))
		self.appendleft(item)

	def get(self):
		return self.pop()

	def empty(self):
		if len(self) == 0:
			return True
		else:
			return False

	def qsize(self):
		return len(self)


class Package(object):

	"""
	send/recv tested with: REQ/REP, REQ/ROUTER, DEALER/ROUTER, DEALER/REP
	"""
	def __init__(self, dest_addr = "", msg = "", encapsulated = None):
		assert isinstance(encapsulated, Package) or encapsulated == None
		self.dest_addr = dest_addr
		self.msg = msg
		self.encapsulated = encapsulated
		

	def __package__(self):
		return [self.dest_addr, "", self.msg, self.encapsulated]

	def __getitem__(self, idx):
		return self.__package__()[idx]

	def __repr__(self):
		return str(self.__dict__)

	def __aslist__(self):
		encapsulated = self[3]
		msg = self[:3]
		while encapsulated:
			msg.extend(encapsulated[:3])
			encapsulated = encapsulated[3]
		return msg

	@staticmethod
	def __from_list__(l):
		l = copy(l)
		l.reverse()
		package = None
		for dest_addr, msg in zip(l[2::3], l[::3]):
			package = Package(dest_addr = dest_addr, msg = msg, encapsulated = package)
		return package

	def unwrap(self, levels = 1):
		package = self
		for i in xrange(levels):
			package = package[3]
		return package

	def reply(self, socket):
		assert socket.TYPE == zmq.ROUTER, 'Reply is only implemented for ROUTER sockets because these need to state the destination address explicitly. If you want to reply on a different socket type, just use Packet.send()'
		assert hasattr(self, 'sender_addr'), 'Cannot reply '
		self.dest_addr = self.sender_addr
		self.send(socket)

	def send(self, socket):
		assert isinstance(socket, zmq.Socket)
		if socket.TYPE == zmq.ROUTER:
			socket.send_multipart(self.__aslist__())
		elif socket.TYPE == zmq.DEALER:
			# print(self.__aslist__()[1:])
			socket.send_multipart(self.__aslist__()[1:])
		elif socket.TYPE == zmq.REQ or socket.TYPE == zmq.REP:
			
			socket.send_multipart(self.__aslist__()[2:])
		else:
			raise Exception('not implemented')

	def show(self):
		return str(self.__aslist__())
	
	

	@staticmethod
	def recv(socket):
		l = socket.recv_multipart()
		if socket.TYPE == zmq.REP or socket.TYPE == zmq.REQ:
			# Tested 
			return Package.__from_list__([None, ''] + l)
		elif socket.TYPE == zmq.ROUTER:
			# Tested
			package = Package.__from_list__([None, ''] + l[2:])
			package.sender_addr = l[0]
			return package 
		elif socket.TYPE == zmq.DEALER:
			return Package.__from_list__([None] + l)
		else:
			raise Exception('not implemented')

	@staticmethod
	def stripped(package):
		"""
		Return a copy of the package, stripped of any encapsulated packages
		"""
		p = copy(package)
		p.encapsulated = None
		return p



class AddressManager(object):

	rcon = Redis()

	_key_prefix = 'endpoint_'
	# endpoints = dict()
	def __init__(self, endpoint_name, protocol, host, port = None):
		AddressManager.register_endpoint(endpoint_name, protocol, host, port)

	@staticmethod
	def register_endpoint(endpoint_name, protocol, host, port = None):
		assert isinstance(endpoint_name, str)
		print('Registered endpoint {}'.format(endpoint_name))
		if protocol == 'tcp' and port == None:
			raise Exception('When using tcp, please also specify a port')
		elif protocol == 'ipc' and port != None:
			raise Exception('When using ipc you should not specify a port')
		key = AddressManager._key_prefix + endpoint_name

		if AddressManager.rcon.get(key):
			print('Overwriting endpoint: {}'.format(endpoint_name))
		AddressManager.rcon.set(key, (protocol, host, port))
		# AddressManager.endpoints[endpoint_name] = (protocol, host, port)

	@staticmethod
	def get_bind_address(endpoint_name):
		if AddressManager.rcon.get(AddressManager._key_prefix + endpoint_name):
			protocol, host, port = eval(AddressManager.rcon.get(AddressManager._key_prefix + endpoint_name))
			return '%s://*:%s'%(protocol, port)
		else:
			return None
	@staticmethod
	def get_connect_address(endpoint_name):
		if AddressManager.rcon.get(AddressManager._key_prefix + endpoint_name):
			protocol, host, port = eval(AddressManager.rcon.get(AddressManager._key_prefix + endpoint_name))
			return '%s://%s:%s'%(protocol, host, port)
		else:
			return None

	@staticmethod
	def registered():
		return len(AddressManager.keys())

	@staticmethod
	def say_something():
		return 'something'



# class BaseAgent(object):
	

# 	__metaclass__ = abc.ABCMeta
# 	def loop(self):
# 		while True:
# 			self.iteration()

# 	@abc.abstractmethod
# 	def iteration(self):
# 		return

# 	@abc.abstractmethod
# 	def setup(self):
# 		return
	
# 	def say(self, msg):
# 		print('{} - {}: {}'.format(datetime.now().strftime('%H:%M:%S'), self.name, msg))

# 	def simulate_crash(self, probability = 0.5):
# 		if random() < probability: 
# 			print(x)

# 	def attach_handler(self, socket, handler):
# 		if not hasattr(self, 'handlers'):
# 			self.handlers = {socket : handler}
# 		else:
# 			if socket in self.handlers.keys() or handler in self.handlers.items():
# 				raise Exception('Handler {} already registered for socket {}'.format(handler, socket))
# 			else:
# 				self.say('Registering handler {} for socket {}'.format(handler, socket))
# 				self.handlers.update({socket : handler})







"""
What do I want to test? It depends. I want to test how many messages per second the broker can process. 
So I make a process which sends a start message to the broker and a stop message
When the broker receives a stop message it exits the main loop
I want to add a socket to the broker that's used for stop/start messages. 
This socket should only be there when I'm running the profiler
The socket should only be registered in the poller when I'm profiling

It can be assumed that all agents are running on the same localhost

Implementation ways:
1) Subclass broker with a TestedBroker. 
2) wrap/decorate the broker
	- before_run: create socket and listen on the socket until it receives "start" from the profiler
	- register socket in broker poller and 
3) make a profiler class 

"""
class Sock(object):
	def __init__(self, name, socket_type, bind, handler):
		assert isinstance(name, str)
		self.name = name
		assert isinstance(socket_type, int)
		self.socket_type = socket_type
		assert isinstance(bind, bool)
		self.bind = bind
		# assert callable(handler) or handler == None
		self.handler = handler
		# assert isinstance(context, zmq.Context)
		# self.socket_obj = context.socket(socket_type)

	def __repr__(self):
		return str(self.__dict__)
		return super(Sock, self).__repr__() + ". Name: '{}'. Endpoint: '{}')".format(self.name, self.endpoint)

	def create_socket(self, context):
		self.socket_obj = context.socket(self.socket_type)
		# self.socket_obj.setsockopt(zmq.RCVHWM, 1000000)

	def set_endpoint(self, endpoint):
		self.endpoint = endpoint
		if self.bind:
			self.address = AddressManager.get_bind_address(endpoint)
		else:
			self.address = AddressManager.get_connect_address(endpoint)

	def connect(self):
		assert hasattr(self, 'socket_obj')
		assert hasattr(self, 'address'), 'Run set_enpoint first.'
		if self.bind:
			self.socket_obj.bind(self.address)
			return 'Bound {} to {}'.format(self.name, self.address)
		else:
			self.socket_obj.connect(self.address)
			return 'Connected {} to {}'.format(self.name, self.address)





	# def __getitem__(self, item):
	# 	return self.item
from multi_key_dict import multi_key_dict
class SocketDict(multi_key_dict):

	def put(self, sock):
		assert isinstance(sock, Sock)
		assert hasattr(sock, 'name')
		assert hasattr(sock, 'endpoint')
		assert hasattr(sock, 'socket_obj')
		self[sock.name, sock.socket_obj] = sock

	def names(self):
		return map(lambda d: d.name, self.values())
		# return map(lambda s: s.gn, self.sockets.values())

	def sockets(self):
		return map(lambda d: d.socket_obj, self.values())



class AgentProcess(Process):

	__metaclass__ = abc.ABCMeta

	def __init__(self, name, endpoints, verbose = False):
		Process.__init__(self)		
		self.context = zmq.Context()
		self.sockets = SocketDict()
		assert isinstance(endpoints, dict)
		self.endpoints = endpoints
		# assert set(self.sockets.names()) == set(endpoints.keys()), "Argument 'endpoints' must be a dictionary with endpoint names as items for each of the keys {}".format(self.sockets.names())

		if not verbose: self.say = lambda x: None
		
		self.name = "{}_{}".format(id(self), name)
		self.poller = zmq.Poller()

	# def init_socket(self, socket_name):
	# 	sock = self.sockets[socket_name]
	# 	sock.socket_obj = self.context.socket(sock.socket_type)
	# 	setattr(self, socket_name, sock.socket_obj)
	# 	if sock.bind:
	# 		address = AddressManager.get_bind_address(sock.endpoint)
	# 		sock.socket_obj.bind(address)
	# 	else:
	# 		address = AddressManager.get_connect_address(sock.endpoint)
	# 		sock.socket_obj.connect(address)
	# 	if sock.handler:
	# 		if not hasattr(self, sock.handler): 
	# 			raise Exception('{} has no handler {}'.format(self.__class__, sock.handler))
	# 		if hasattr(self, 'poller') and isinstance(self.poller, zmq.Poller):
	# 			self.poller.register(sock.socket_obj, zmq.POLLIN)
	# 		else:
	# 			self.poller = zmq.Poller()
	# 			self.poller.register(sock.socket_obj, zmq.POLLIN)

	

	# def attach_handler(self, socket, handler):
	# 	if socket in self.handlers.keys() or handler in self.handlers.items():
	# 		raise Exception('Handler {} already registered for socket {}'.format(handler, socket))
	# 	else:
	# 		self.say('Registering handler {} for socket {}'.format(handler, socket))
	# 		self.handlers.update({socket : handler})
	# 		# print(socket)
	# 		# print(self.sockets)
	# 		self.poller.register(socket, zmq.POLLIN)

		
	def say(self, msg):
		# if not (re.match('.*{}.*'.format(MsgCode.PING), str(msg)) or re.match('.*{}.*'.format(MsgCode.PONG), str(msg))):
		print('{} - {}: {}'.format(datetime.now().strftime('%H:%M:%S'), self.name, msg))
		 # or not re.match('.*{}.*'.format(MsgCode.PONG), msg):
			

	def simulate_crash(self, probability = 0.5):
		if random() < probability: 
			print(x)

	# def attach_handler(self, socket, handler):
	# 	if socket in self.handlers.keys() or handler in self.handlers.items():
	# 		raise Exception('Handler {} already registered for socket {}'.format(handler, socket))
	# 	else:
	# 		self.say('Registering handler {} for socket {}'.format(handler, socket))
	# 		self.handlers.update({socket : handler})
	# 		# print(socket)
	# 		# print(self.sockets)
	# 		self.poller.register(socket, zmq.POLLIN)

	# def new_socket(self, endpoint, socket_name, socket_type, bind = False, handler = None):
	# 	"""
	# 	:param sendpoint: where the socket should bind/connect to:
	# 	:param socket_name: frontend, backend, etc.
	# 	"""
	# 	# assert isinstance(socket, zmq.Socket)
	# 	assert isinstance(endpoint, str)
	# 	assert isinstance(bind, bool)
	# 	socket = self.context.socket(socket_type)
	# 	self.sockets[socket_name] = socket
	# 	setattr(self, socket_name, socket)
	# 	if bind:
	# 		address = AddressManager.get_bind_address(endpoint)
	# 		self.say('New socket. Name: {}. Role: {}.  Type {}. Binding to {}'.format(endpoint, socket_name, socket_type, address))
	# 		socket.bind(address)
	# 	else:
	# 		address = AddressManager.get_connect_address(endpoint)
	# 		self.say('New socket. Name: {}. Role: {}.  Type {}. Binding to {}'.format(endpoint, socket_name, socket_type, address))
	# 		socket.connect(address)

	# 	if handler:
	# 		self.attach_handler(socket, handler)
			
		
	# def close_sockets(self):
	# 	for socket_name, socket in self.sockets.items():
	# 		socket.close()

	# def poll_sockets(self):
	# 	for socket in dict(self.poller.poll(10)):
	# 		self.handlers[socket]()

	
	@abc.abstractmethod
	def setup(self):
		return

	@abc.abstractmethod
	def iteration(self):
		return

	
	def init_socket(self, sock):
		self.say('INIT socket {}'.format(sock))
		if hasattr(self, sock.name):
			print('ASWKDJHASD')
		sock.set_endpoint(self.endpoints[sock.name])
		sock.create_socket(self.context)
		self.say(sock.connect())
		if self.sockets.has_key(sock.name):
			del self.sockets[sock.name]
		self.sockets.put(sock)
		setattr(self, sock.name, sock.socket_obj)
		self.start_socket_poll(sock.name)

	def init_all_sockets(self):
		self.say('Endponts: '.format(self.endpoints))
		for sock in self.__sockets__:
			self.init_socket(sock)
			# self.say('Starting socket: {}'.format(sock))
			

	def start_socket_poll(self, socket_name):
		"""
		Connects socket to a handler function and registers in poller
		"""
		sock = self.sockets[socket_name]
		# self.say('Starting socket: {}'.format(sock))
		# if sock.bind:
		# 	self.say('Binding to {}'.format(sock.address))
		# 	sock.socket_obj.bind(sock.address)
		# else:
		# 	self.say('Connecting to {}'.format(sock.address))
		# 	sock.socket_obj.connect(sock.address)
		if sock.handler:
			if isinstance(sock.handler, str):
				if not hasattr(self, sock.handler): 
					raise Exception('{} has no handler {}'.format(self.__class__, sock.handler))
				sock.handler = getattr(self, sock.handler)
			self.say('Registering socket in poller: {}'.format(sock.socket_obj))
			self.poller.register(sock.socket_obj, zmq.POLLIN)

			# if hasattr(self, 'poller') and isinstance(self.poller, zmq.Poller):
			# 	self.poller.register(sock.socket_obj, zmq.POLLIN)
			# else:
			# 	self.poller = zmq.Poller()
			# 	self.poller.register(sock.socket_obj, zmq.POLLIN)
		else:
			self.say('No handler attached')


	def poll_sockets(self):
		sockets = True
		# self.say('In poll')
		while sockets:
			sockets = dict(self.poller.poll(1))
			# self.say(str(sockets))
			for socket in self.sockets.sockets():
				# print(self.sockets[socket].name)
				if socket in sockets:
					self.sockets[socket].handler()

	def run(self):
		"""
		Main routine
		"""
		self.init_all_sockets()
		self.setup()
		while True:
			self.iteration()	
		
		# self.backend.close()
		# self.frontend.close()

# from threading import Thread

class TeztAgent(AgentProcess):

	__sockets__ = [Sock('backend', zmq.DEALER, False, 'backend_handler')]
	
	def backend_handler(self):
		m = self.backend.recv_multipart()
		# self.say(str(m))

	def iteration(self):
		# print('Iterate')
		self.poll_sockets()
		# Thread(target = self.poll_sockets).start()
		# self.poll_sockets()
		order = 'new order {}'.format(random())
		package = Package(msg = order)
		self.say('Sending on backend: {}'.format(package))
		package.send(self.backend)
		# sleep(1)

		
	

	def setup(self):
		pass

class TeztAgentReconnect(AgentProcess):

	__sockets__ = [Sock('backend', zmq.DEALER, False, 'backend_handler')]
	
	def backend_handler(self):
		m = self.backend.recv_multipart()
		# self.say(str(m))

	def iteration(self):
		# print('Iterate')
		# self.poll_sockets()
		# Thread(target = self.poll_sockets).start()
		# self.poll_sockets()
		order = 'new order {}'.format(random())
		package = Package(msg = order)
		self.say('Sending on backend: {}'.format(package))
		package.send(self.backend)
		# sleep(1)
		self.init_socket(self.sockets['backend'])
		self.say('Done initializing')
	
	

	def setup(self):
		pass



# class AgentThread(Thread, BaseAgent):

# 	__metaclass__ = abc.ABCMeta

# 	def __init__(self, context, name, alive_event):
# 		assert isinstance(context, zmq.Context)
# 		Thread.__init__(self)
# 		self.context = context
# 		self.name = "{}_{}".format(id(self), name)
# 		self.alive_event = alive_event
# 		# self.start()

# 	@abc.abstractmethod
# 	def run(self):
# 		"""
# 		Main routine
# 		"""
# 		return

class Auction(AgentProcess):

	def run(self):
		backend = self.context.socket(zmq.DEALER)
		backend.connect(AddressManager.get_connect_address('db_frontend'))
		poller = zmq.Poller()
		poller.register(backend, zmq.POLLIN)
		while True:
			sockets = dict(poller.poll(100))
			if backend in sockets:
				reply = backend.recv_multipart()
			backend.send_multipart(["", 'new transaction', "", ""])
			sleep(random()/1)










class Order:

	code = "ORDER"

	def __init__(self, owner_id, price, volume):
		self.owner_id = owner_id
		self.price = price
		self.volume = volume

	def __repr__(self):
		return "{} {} {} {}".format(Order.CODE, self.owner, self.price, self.volume)


