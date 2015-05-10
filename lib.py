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


Socket = namedtuple('Socket', 'socket name bind')


class AgentProcess(Process):

	__metaclass__ = abc.ABCMeta

	def __init__(self, name, frontend = None, backend = None, verbose = False, n_iterations = -1):
		Process.__init__(self)
		if not verbose: self.say = lambda x: None
		self.context = zmq.Context()
		self.name = "{}_{}".format(id(self), name)
		self.frontend_name = frontend
		self.backend_name = backend
		self.sockets = {}
		self.handlers = {}
		
	@abc.abstractmethod
	def run(self):
		"""
		Main routine
		"""
		return


	@abc.abstractmethod
	def setup(self):
		return
	
	def say(self, msg):
		print('{} - {}: {}'.format(datetime.now().strftime('%H:%M:%S'), self.name, msg))

	def simulate_crash(self, probability = 0.5):
		if random() < probability: 
			print(x)

	def attach_handler(self, socket_name, handler):
		if socket_name in self.handlers.keys() or handler in self.handlers.items():
			raise Exception('Handler {} already registered for socket {}'.format(handler, socket_name))
		else:
			# self.say('Registering handler {} for socket_name {}'.format(handler, socket_name))
			self.handlers.update({socket_name : handler})

	def new_socket(self, socket_name, socket_role, socket_type, bind = False, handler = None):
		"""
		:param socket_role: frontend, backend, etc.
		"""
		# assert isinstance(socket, zmq.Socket)
		assert isinstance(socket_name, str)
		assert isinstance(bind, bool)
		socket = self.context.socket(socket_type)
		self.sockets[socket_name] = socket
		setattr(self, socket_role, socket)
		if bind:
			address = AddressManager.get_bind_address(socket_name)
			self.say('New socket. Name: {}. Role: {}.  Type {}. Binding to {}'.format(socket_name, socket_role, socket_type, address))
			socket.bind(address)
		else:
			address = AddressManager.get_bind_address(socket_name)
			self.say('New socket. Name: {}. Role: {}.  Type {}. Binding to {}'.format(socket_name, socket_role, socket_type, address))
			socket.connect(address)

		if handler:
			self.attach_handler(socket, handler)
			# if socket in self.handlers.keys() or handler in self.handlers.items():
			# 	raise Exception('Handler {} already registered for socket {}'.format(handler, socket))
			# else:
			# 	self.say('Registering handler {} for socket {}'.format(handler, socket))
			# 	self.handlers.update({socket : handler})			
		
	def close_sockets(self):
		for socket_name, socket in self.sockets.items():
			socket.close()



		# self.sockets[name] = Socket(socket = socket, name = name, bind = bind)

	# self.new_socket(self.frontend_name, zmq.ROUTER, bind = True)

	# self.frontend = self.context.socket(zmq.ROUTER)
	# self.frontend.bind(AddressManager.get_bind_address(self.frontend_name))
	# self.attach_handler(self.frontend, self.handle_frontend)

	def connect_socket(self, name):
		self.socket[name].socket


	def handle_sockets(self):
		for socket in dict(self.poller.poll(10)):
			self.handlers[socket]()

	

	def run(self):
		self.setup()
		while True:
			self.handle_sockets()
			self.iteration()	

		for socket in self.sockets():
			socket.close()
		# self.backend.close()
		# self.frontend.close()

import zmq
from heapdict import heapdict
from brokers import Job
from traders import Trader
from datetime import datetime, timedelta
from bidict import bidict
import abc

class PingPongBrokerNew(AgentProcess):
	WORKER_EXPIRE_SECONDS = 3

	def setup(self):
		self.new_socket(self.frontend_name, 'frontend', zmq.ROUTER, bind = True, handler = self.handle_frontend)
		self.new_socket(self.backend_name, 'backend', zmq.ROUTER, bind = True, handler = self.handle_backend)
		
		self.poller = zmq.Poller()
		for socket_name, socket in self.sockets.items():
			self.poller.register(socket, zmq.POLLIN)
		
		self.workers = heapdict()
		self.jobs = DQueue(item_type = Job)

		self.jobs_received = 0

	def expire_workers(self):
		if len(self.workers) > 0:
			now = datetime.now()
			while len(self.workers) > 0 and self.workers.peekitem()[1] < now:
				self.say('Expiring worker')
				self.workers.popitem()

	def add_worker(self, worker_addr):
		expires = datetime.now() + timedelta(seconds = self.WORKER_EXPIRE_SECONDS)
		self.workers[worker_addr] = expires

	def remove_worker(self, worker_addr):
		"""
		This doesn't remove the worker from the priority queue, but it doesn't matter since 
		the worker will eventually be removed when it was supposed to expire anyway
		"""
		self.say('Removing worker: {}'.format(worker_addr))
		if self.workers.has_key(worker_addr):
			del self.workers[worker_addr]

	
	def send_job(self):
		worker_addr = self.workers.popitem()[0]
		job = self.jobs.get()
		client_p = Package(dest_addr = job.client)
		package = Package(dest_addr = worker_addr, msg = job.work, encapsulated = client_p)
		self.say('sending on backend: {}'.format(package))
		package.send(self.backend)


	def handle_frontend(self):
		package = Package.recv(self.frontend)
		self.say('On frontend: {}'.format(package))
		job = Job(client=package.sender_addr, work=package.msg)
		self.jobs_received += 1
		self.jobs.put(job)

	def handle_backend(self):
		package = Package.recv(self.backend)
		self.say('On backend: {}'.format(package))
		worker = package.sender_addr
		if package.msg == MsgCode.DISCONNECT:
			self.remove_worker(worker)
		else:
			self.add_worker(worker)
			# Send PONG to worker
			Package(dest_addr = worker, msg = MsgCode.PONG).send(self.backend)
		
		if package.msg == MsgCode.JOB_COMPLETE:
			if package.encapsulated:
				### Forward result from worker to client
				self.say('Sending on frontend: {}'.format(package.encapsulated))
				package.encapsulated.send(self.frontend)

	
	
	def iteration(self):
		self.handle_sockets()
		if not self.jobs.empty() and len(self.workers) > 0:
			self.send_job()
		self.expire_workers()

	def run(self):
		self.setup()
		while True:
			self.handle_sockets()
			self.iteration()	
		
		self.backend.close()
		self.frontend.close()



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


