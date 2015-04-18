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

STATUS_READY = "1"
ORDER_RECEIVED = "2"

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




"""
1) 
"""

class AgentProcess(Process):

	__metaclass__ = abc.ABCMeta

	def __init__(self, context, name):
		assert isinstance(context, zmq.Context)
		Process.__init__(self)
		self.context = context
		self.name = "{}_{}".format(id(self), name)
		

	@abc.abstractmethod
	def run(self):
		"""
		Main routine
		"""
		return

	def register_at_DNS(self):
		pass

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




class REQWorkerThread(AgentThread):
		
	def __init__(self, context, name, alive_event):
		super(REQWorkerThread, self).__init__(context, name, alive_event)
		self.setup()

	def do_work(self):
		sleep(1)
		return sum(range(1000))

	def fail_randomly(self):
		if random() < 0.9: 
			message = 'ready'
		else: 
			message = 'failed'

	def setup(self):
		self.frontend = self.context.socket(zmq.REQ)
		self.frontend.connect(AddressManager.get_connect_address('db_backend'))
		self.poller = zmq.Poller()
		


	def run(self):
		self.poller.register(self.frontend, zmq.POLLIN)
		self.frontend.send_multipart(["ready", "", ""])	
		while self.alive_event.isSet():
			sockets = dict(self.poller.poll(100))
			if self.frontend in sockets:
				request = self.frontend.recv_multipart()
				client_id, empty, workload = request[0:3]
				if workload == 'quit': break
				self.do_work()		
				# self.frontend.send_multipart(['job complete', empty, client_id])
				message = WorkerMsg(STATUS_READY, client_id, ORDER_RECEIVED)
				# self.frontend.send_multipart(['job complete', empty, client_id])

		self.frontend.close()



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

class WorkerMsg(Message):
	""" Message sent from worker to client via a broker """
	def __init__(self, status, client_addr, client_msg):		
		self.message = [status, "", client_addr, "", client_msg]


# class ClientMsg(Message):


class BrokerWithQueueing(AgentProcess):

	def __init__(self, context, name, frontend_name, backend_name):
		super(BrokerWithQueueing, self).__init__(context, name)
		self.frontend_name = frontend_name
		self.backend_name = backend_name

	def run(self):
		frontend = self.context.socket(zmq.ROUTER)
		frontend.bind(AddressManager.get_bind_address(self.frontend_name))
		backend = self.context.socket(zmq.ROUTER)
		backend.bind(AddressManager.get_bind_address(self.backend_name))

		poller = zmq.Poller()
		poller.register(backend, zmq.POLLIN)
		poller.register(frontend, zmq.POLLIN)
		
		workers = Queue.Queue()
		jobs = Queue.Queue()

		while True:
			
			sockets = dict(poller.poll(1000))	

			if sockets: self.say('pending jobs: {}, workers in pool: {}'.format(jobs.qsize(), workers.qsize()))

			if backend in sockets:

				# request = backend.recv_multipart()
				# # self.say('from worker: {}'.format(request))
				# worker_id, message, client_id = request[0], request[2], request[4]
				request = backend.recv_multipart()
				# self.say('from worker: {}'.format(request))
				worker_id, message, client_id = request[0], request[2], request[4]

				message, client_id, client_msg

				if message in ["ready", "job complete"]:
					workers.put(worker_id)
					# self.say('workers in pool: {}'.format(workers.qsize()))
				if client_id: 
					frontend.send_multipart([client_id, "", message])
				
			if frontend in sockets:
				request = frontend.recv_multipart()
				client_id, workload = request[0], request[2]
				jobs.put((client_id, workload))
			
			if not jobs.empty() and not workers.empty():
				worker_id = workers.get()
				client_id, workload = jobs.get()
				backend.send_multipart([worker_id, "", client_id, "", workload])

			if jobs.qsize() > 10:
				pass

		backend.close()
		frontend.close()



class BrokerWithPool(BrokerWithQueueing):
	def __init__(self, context, name, frontend_name, backend_name, n_workers_start = 2, n_workers_max = 10):
		assert isinstance(context, zmq.Context)
		super(BrokerWithPool, self).__init__(context, name, frontend_name, backend_name)
		self.alive_event = Event()
		self.alive_event.set()
		self.n_workers_start = n_workers_start
		self.n_workers_max = n_workers_max
		self.context = context
		self.started_workers = 0

	def start_worker(self):
		self.say('Starting worker...')
		REQWorkerThread(self.context, '{}_{}'.format(self.name, 'worker'), self.alive_event).start()	
		self.started_workers += 1

	def run(self):
		for j in xrange(self.n_workers_start): self.start_worker()
		try:
			super(BrokerWithPool, self).run()
		except KeyboardInterrupt:
				self.alive_event.clear()	



# class BrokerWithQueueing(AgentProcess):

# 	def __init__(self, name, context, frontend_name, backend_name, pool_name = None):
# 		super(BrokerWithQueueing, self).__init__(name, context)
# 		self.frontend_name = frontend_name
# 		self.backend_name = backend_name
# 		self.setup()

# 	def setup(self):
# 		self.frontend = self.context.socket(zmq.ROUTER)
# 		self.frontend.bind(AddressManager.get_bind_address(self.frontend_name))
# 		self.backend = self.context.socket(zmq.ROUTER)
# 		self.backend.bind(AddressManager.get_bind_address(self.backend_name))

		
		
# 		self.workers = Queue.Queue()
# 		self.jobs = Queue.Queue()

# 	def run(self):
# 		poller = zmq.Poller()
# 		poller.register(self.backend, zmq.POLLIN)
# 		poller.register(self.frontend, zmq.POLLIN)

# 		while True:
# 			print('pending jobs: {}, workers in pool: {}'.format(self.jobs.qsize(), self.workers.qsize()))
# 			print('ads')
# 			sockets = dict(poller.poll(1000))	
						
# 			if self.backend in sockets:
# 				request = self.backend.recv_multipart()
# 				# self.say('from worker: {}'.format(request))
# 				worker_id, message, client_id = request[0], request[2], request[4]
# 				if message in ["ready", "job complete"]:
# 					self.workers.put(worker_id)
# 					self.say('workers in pool: {}'.format(self.workers.qsize()))
# 				if client_id: self.frontend.send_multipart([client_id, "", message])

# 			if self.frontend in sockets:
# 				request = self.frontend.recv_multipart()
# 				client_id, workload = request[0], request[2]
# 				self.jobs.put((client_id, workload))
				
			
# 			if not self.jobs.empty() and not self.workers.empty():
# 				worker_id = self.workers.get()
# 				client_id, workload = self.jobs.get()
# 				self.backend.send_multipart([worker_id, "", client_id, "", workload])

# 			if self.jobs.qsize() > 10:
# 				pass

# 		self.backend.close()
# 		self.frontend.close()

