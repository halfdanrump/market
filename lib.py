from __future__ import print_function
from multiprocessing import Process
from threading import Thread, Event
import abc
from redis import Redis
import zmq
import Queue
from time import sleep
from random import random
from datetime import datetime
from collections import namedtuple
from copy import copy
STATUS_READY = "READY"
ORDER_RECEIVED = "ORDER VERIFIED"
JOB_COMPLETE = "JOB COMPLETE"
INVALID_ORDER = "INVALID ORDER"

# Package = namedtuple('Package', 'addr msg encapsulated')

class Package:
	def __init__(self, addr, msg, encapsulated = None):
		self.addr = addr
		self.msg = msg
		self.package = [addr, "", msg, encapsulated]

	def __getitem__(self, idx):
		return self.package[idx]

	def unwrap(self, levels = 1):
		package = self
		for i in xrange(levels):
			package = package[3]
		return package

	def send(self):
		encapsulated = self[3]
		msg = self[:3]
		while encapsulated:
			msg.extend(encapsulated[:3])
			encapsulated = encapsulated[3]
		return msg
	
	@staticmethod
	def from_list(l):
		l = copy(l)
		l.reverse()
		package = None
		for addr, msg in zip(l[::3], l[2::3]):
			package = Package(addr, msg, package)
		return package

	def show(self):
		return str(self.send())

	def __repr__(self):
		if not self[3]:
			return str(self[:3])
		else:
			return str(self[:3])[:-1] + ', [...]]'




class PackageSimple(object):
	def __init__(self, addr, msg, encapsulated = None):
		self.addr = addr
		self.msg = msg
		self.package = [addr, "", msg, encapsulated]

	def unwrap(self):
		return self.package[3]

	def send(self):
		encapsulated = self.package[3]
		msg = self.package[:3]
		while encapsulated:
			msg.extend(encapsulated.package[:3])
			encapsulated = encapsulated.package[3]
			return msg
	
	@staticmethod
	def from_list(l):
		l = copy(l).reverse()
		for i, (addr, msg) in enumerate(zip(l[::3], l[2::3])):
			if i == 0:
				package = Package(addr, msg)
			else:
				package = Package(addr, msg, package)
		return package

	def __repr__(self):
		return str(self.package)



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


zmqSocket = namedtuple('zmqSocket', 'type bind')


class AgentProcess(Process):

	__metaclass__ = abc.ABCMeta

	# frontend = abc.abstractproperty()
	# @abc.abstractproperty
	# def sockets(frontend, backend): 
	# 	assert isinstance(frontend, zmqSocket) or None
	# 	assert isinstance(backend, zmqSocket) or None

	def __init__(self, name, frontend = None, backend = None):
		# assert isinstance(frontend, zmqSocket) or None
		# assert isinstance(backend, zmqSocket) or None
		Process.__init__(self)
		self.context = zmq.Context()
		self.name = "{}_{}".format(id(self), name)
		self.frontend_name = frontend
		self.backend_name = backend
		
	@abc.abstractmethod
	def run(self):
		"""
		Main routine
		"""
		return

	def say(self, msg):
		print('{} - {}: {}'.format(datetime.now().strftime('%H:%M:%S'), self.name, msg))
 



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







class WorkerMsg:
	""" Message sent from worker to client via a broker 
	:param status: Contains the status code of the worker, e.g. READY or SHUTTING_DOWN
	:param client_addr: The address of the original client (not broker) that requsted the work
	:param client_msg: Message to the client, such as result of calculation. 
	"""
	def __init__(self, status, client_addr = "", client_msg = ""):
		# assert int(status) in range(100), 'Status should be an integer code'
		assert len(client_addr) == 5 or client_addr == "", client_addr
		self.status = status
		self.client_addr = client_addr
		self.client_msg = client_msg
		self.message = [status, "", client_addr, "", client_msg]

	@staticmethod
	def from_mulipart(l, socket_type):
		msg = WorkerMsg(*l[2::2])
		if socket_type == zmq.ROUTER:
			msg.worker_addr = l[0]
		return msg


class ClientMsg:
	# def __init__(self, client_addr = None, client_msg = None):
	def __init__(self, client_msg, client_addr = ""):
		assert len(client_addr) >= 5 or client_addr == "", client_addr
		self.client_addr = client_addr
		self.client_msg = client_msg
		# self.message = [client_addr, "", client_msg]
		self.message = [client_msg, "", client_addr]

	@staticmethod
	def to_multipart(self, socket_type):
		pass

	@staticmethod
	def from_multipart(l, socket_type):
		# print('ASLDKJAS')
		# print(l)
		# print("HUAR")
		# print(l[2::2])
		msg = ClientMsg(*l[::2])
		# if socket_type == zmq.ROUTER:
		# 	msg.client_addr = l[0]
		return msg		



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

	# @staticmethod
	# def from_str(string):
	# 	try:



class BrokerWorker:
	# [worker_id, "", client_id, "", workload]
	pass

class WorkerBroker:
	# Worker: ["ready", "", ""]
	# Worker: ['job complete', empty, client_id]
	# Auth: ["job complete", "", trader_id]
	pass

class ClientBroker:
	# Trader: ["", order] ()
	# Auth: [trader_id, "", order] (auth is proxy so needs to send trader_id)
	pass


from copy import copy

class Message:

	__metaclass__ = abc.ABCMeta

	def zmqs(self, socket_type = None):
		if socket_type == zmq.DEALER:
			msg = copy(self.message)
			self.message.insert(0, "")
			return msg
		else:
			return self.message
	
	def recv(self, socket_type):
		# if socket_type == zmq.ROUTER:
		return self.message[::2]


class Msg(object):

	SENDER = b"0"
	RECIPIENT = b"1"
	PAYLOAD = b"2"

	def __init__(self, sender, recipient, payload):
		self.__dict__.update({'sender': sender, 'recipient': recipient, 'payload': payload})

	def wrap(self, msg):
		pass



# class SusperSocket(zmq.Socket):

# 	def __init__(self, name, socket_type, bind = False):
# 		self.type = socket_type
# 		self = 
# 		if socket.bind


