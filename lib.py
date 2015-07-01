from __future__ import print_function
from zmq.eventloop import ioloop, zmqstream
from multiprocessing import Process
import abc
from redis import Redis
import zmq
from time import sleep
from random import random
from datetime import datetime
from copy import copy
from collections import deque

class MsgCode:
	STATUS_READY = "READY"
	ORDER_RECEIVED = "ORDER VERIFIED"
	ORDER_STORED = "ORDER_STORED"
	SUCCESS = "SUCCESS"
	ERROR = "ERROR"
	JOB_COMPLETE = "JOB COMPLETE"
	INVALID_ORDER = "INVALID ORDER"
	PING = "PING"
	PONG = "PONG"
	DISCONNECT = "DISCONNECT"
	QUEUE_FULL = "FULL"
	WORK = "WORK"



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


class Agent(Process):

	__metaclass__ = abc.ABCMeta

	def __init__(self, name, endpoints):
		Process.__init__(self)
		### Make assertions about endpoints
		self.endpoints = endpoints		
		self.context = zmq.Context()
		self.loop = ioloop.IOLoop.instance()
		
	def get_endpoint(self, name):
		return self.endpoints[name]

	def stream(self, name, socket_type, bind, handler):
		""" create a new stream """
		socket = self.context.socket(socket_type)
		# print(socket)
		endpoint = self.get_endpoint(name)
		if bind: 
			address = AddressManager.get_bind_address(endpoint)
			socket.bind(address)
		else:
			address = AddressManager.get_connect_address(endpoint)
			socket.connect(address)
		stream = zmqstream.ZMQStream(socket)
		stream.on_recv(handler)	
		# setattr(self, name + '_stream', stream)
		# setattr(self, name + '_socket', socket)
		return stream, socket
	
	def simulate_overload(self, probability = 0.1):
		""" Simulate CPU overload by sleeping for some time. """
		if random() < probability: 
			self.say('I am busy')
			sleep(5)

	def simulate_crash(self, probability = 0.1):
		""" Simulate crash forcing IndexError"""
		if random() < probability: 
			l = list()
			l[0]

	def run(self):
		""" overrides Process.run(). This will be called when start() is called """
		self.say('Setting up agent...')
		self.setup()
		self.say('Starting ioloop...')
		self.loop.start()

	def say(self, msg):
		print('{} - {}: {}'.format(datetime.now().strftime('%H:%M:%S'), self.name, msg))

	@abc.abstractmethod
	def setup(self):
		""" Will usually be run a single time when the process is started. Should be used to create sockets, bind handlers, create instance variables and so on. """
		return

