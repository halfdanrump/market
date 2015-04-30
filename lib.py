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
			assert isinstance(item, self.item_type), 'Expected item of type {} but got {}'.format(self.item_type.__class__, type(item))
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
		return '<Package> {} from: {}, to: {}'.format(self.msg, getattr(self, 'sender_addr', None), getattr(self, 'dest_addr', None))

		if not self[3]:
			return str(self[:3])
		else:
			return str(self[:3])[:-1] + ', [...]]'

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
		# return Package(msg = package.msg, dest_addr = package.dest_addr)

	




# class PackageSimple(object):
# 	def __init__(self, dest_addr, msg):
# 		self.addr = addr
# 		self.msg = msg
# 		self.package = [addr, "", msg]

# 	def append(package):
# 		self.package.append(package)

# 	def unwrap(self, levels = 1):
# 		return self.package[3]

# 	def send(self):
# 		return self.package
# 		# encapsulated = self.package[3]
# 		# msg = self.package[:3]
# 		# while encapsulated:
# 		# 	msg.extend(encapsulated.package[:3])
# 		# 	encapsulated = encapsulated.package[3]
# 		# 	return msg
	
# 	@staticmethod
# 	def from_list(l):
# 		l = copy(l).reverse()
# 		for i, (msg, addr) in enumerate(zip(l[::3], l[2::3])):
# 			print(msg, addr)
# 			if i == 0:
# 				package = Package(addr, msg)
# 			else:
# 				package = Package(addr, msg, package)
# 		return package

# 	def __repr__(self):
# 		return str(self.package)



# def test():

# 	client_msg = Package(addr=client_addr, msg=db_result)
# 	auth_msg = Package(addr=auth_addr, msg=status, encapsulated=client_msg)
# 	trader_msg = 

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


zmqSocket = namedtuple('zmqSocket', 'name type endpoint bind')


class AgentProcess(Process):

	__metaclass__ = abc.ABCMeta

	# frontend = abc.abstractproperty()
	# @abc.abstractproperty
	# def sockets(frontend, backend): 
	# 	assert isinstance(frontend, zmqSocket) or None
	# 	assert isinstance(backend, zmqSocket) or None

	def __init__(self, name, frontend = None, backend = None, verbose = False):
		# assert isinstance(frontend, zmqSocket) or None
		# assert isinstance(backend, zmqSocket) or None
		Process.__init__(self)
		self.context = zmq.Context()
		self.name = "{}_{}".format(id(self), name)
		self.frontend_name = frontend
		self.backend_name = backend

		

		if not verbose: self.say = self.shut_up
		
	@abc.abstractmethod
	def run(self):
		"""
		Main routine
		"""
		return

	def shut_up(self, msg):
		pass

	def say(self, msg):
		print('{} - {}: {}'.format(datetime.now().strftime('%H:%M:%S'), self.name, msg))

	def simulate_crash(self, probability = 0.5):
		if random() < probability: 
			print(x)
 


class MDClient(AgentProcess):
	

	RETRIES = 3
	TIMEOUT = 2500

	# def __init__(self, **kwargs):
	# 	super(MDClient, self).__init__(**kwargs)

	# @abc.abstractmethod



	def reconnect(self):
		pass

	def send(self, service, package):
		assert isinstance(package, Package)
		pass

	def destroy(self):
		pass


"""
 from db_worker to trader:
 [status_msg/order_receipt, "", client_addr]
 SHOULD BE
 [status_msg, "", client_addr, "", order_receipt]



 from trader to worker:
 ["order"]

"""



class AgentThread(Thread):

	__metaclass__ = abc.ABCMeta

	def __init__(self, context, name, alive_event):
		assert isinstance(context, zmq.Context)
		Thread.__init__(self)
		self.context = context
		self.name = "{}_{}".format(id(self), name)
		self.alive_event = alive_event
		# self.start()

	@abc.abstractmethod
	def run(self):
		"""
		Main routine
		"""
		return

	def register_at_DNS(self):
		pass

	def say(self, msg):
		# pass
		print('{} - {}: {}'.format(datetime.now().strftime('%H:%M:%S'), self.name, msg))



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


