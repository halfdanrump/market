import zmq
from zmq.eventloop import ioloop, zmqstream
from multiprocessing import Process
import abc
from datetime import datetime
from lib import AddressManager, MsgCode
from random import random

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
		# if hasattr(self, name + '_stream'): 
			# del eval(getattr(self, name + '_stream'))
		# if hasattr(self, name + '_socket'): 
		# 	del getattr(self, name + '_socket')
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
	
	def simulate_crash(self, probability = 0.1):
		if random() < probability: 
			self.say('I am busy')
			sleep(5)

	def run(self):
		self.say('Setting up agent...')
		self.setup()
		self.say('Starting ioloop...')
		self.loop.start()

	def say(self, msg):
		print('{} - {}: {}'.format(datetime.now().strftime('%H:%M:%S'), self.name, msg))

	@abc.abstractmethod
	def setup(self):
		return
		
	

class Server(Agent):
	def handle_socket(self, msg):
		address, m = msg[0], msg[2]
		self.say(m)
		self.socket.send_multipart([address, '', 'REQUEST OK'])
	
	def setup(self):
		self.stream, self.socket = self.stream('backend', zmq.ROUTER, True, self.handle_socket)


from time import sleep

class Client(Agent):
	i = 0

	def handle_socket(self, msg):
		self.say("{} ,".format(self.i) + "".join(msg))
		self.i += 1
		self.socket.send_multipart(["", 'NEW REQUEST'])

	def reconnect(self):
		self.say('Reconnecting...')
		# self.stream.flush()
		# self.stream.close()
		# self.socket.close()
		# sleep(3)
		del self.stream
		del self.socket
		self.setup()
	
	def setup(self):
		ioloop.DelayedCallback(self.reconnect, 3000, self.loop).start()
		self.stream, self.socket = self.stream('frontend', zmq.DEALER, False, self.handle_socket)
		self.socket.send_multipart(["", 'NEW REQUEST'])



from datetime import datetime, timedelta
from heapdict import heapdict

class PingPongBroker(Agent):
	WORKER_EXPIRE_SECONDS = 3

	# __sockets__ = [
	# Sock('frontend', zmq.ROUTER, bind = True, handler = 'handle_frontend'),
	# Sock('backend', zmq.ROUTER, bind = True, handler = 'recv_from_worker')
	# ]

	def setup(self):
		# self.stream('frontend', zmq.ROUTER, True, self.handle_client_msg)
		self.backend_stream, self.backend_socket = self.stream('backend', zmq.ROUTER, True, self.handle_worker_msg)
		self.workers = heapdict()
		
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

	def handle_worker_msg(self, msg):
		worker_addr, payload = msg[0], msg[2]
		self.say('On backend: {}'.format(payload))
		self.simulate_crash()
		self.add_worker(worker_addr)
		self.backend_socket.send_multipart([worker_addr, "", MsgCode.PONG])

class PingPongWorker(Agent):
	i = 0

	BROKER_ALIVENESS = 3
	BROKER_TIMEOUT_MSEC = 1000
	WAIT_TO_RECCONECT_MSEC = 1000

	def handle_broker_msg(self, msg):
		self.say("{} ,".format(self.i) + "".join(msg))
		self.i += 1
		task = msg[1]
		self.reset_aliveness()
		self.update_broker_timeout()

		if not task == MsgCode.PONG:
			result = self.do_work(task)
			self.socket.send_multipart(["", result])
			self.broker_timer.start()

	def update_broker_timeout(self):
		self._broker_timeout = datetime.now() + timedelta(seconds = self.BROKER_TIMEOUT_MSEC / 1000.0)

	# def timed_ping(self):
	# 	if self._pong_received:
	# 		self.socket.send_multipart(["", MsgCode.PING])
	# 		self._pong_received = False
	# 		self.broker_timer = ioloop.DelayedCallback(self.decrement_aliveness, self.BROKER_TIMEOUT_MSEC, self.loop)
	# 		self.broker_timer.start()

	def timed_ping(self):
		if datetime.now() > self._broker_timeout:
			self._broker_aliveness -= 1
		if self._broker_aliveness == 1:
			self.reconnect()
		else:
			self.socket.send_multipart(["", MsgCode.PING])
			self.broker_timer = ioloop.DelayedCallback(self.timed_ping, self.BROKER_TIMEOUT_MSEC, self.loop)
			self.broker_timer.start()

	def reset_aliveness(self):
		self._broker_aliveness = self.BROKER_ALIVENESS

	def reconnect(self):
		self.say('Reconnecting...')
		# self.stream.flush()
		# self.stream.close()
		# self.socket.close()
		# sleep(3)
		del self.stream
		del self.socket
		self.setup()

	# def reset_timer(self):
	# 	print('reset timer')
	# 	if hasattr(self, 'broker_timer'):
	# 		self.broker_timer.stop()
	# 	self.broker_timer = ioloop.DelayedCallback(self.decrement_aliveness, self.BROKER_TIMEOUT_MSEC, self.loop)
	# 	self.broker_timer.start()

	# def update_aliveness(self):
	# 	self.broker_aliveness -= 1
	# 	self.broker_timer = ioloop.DelayedCallback(self.decrement_aliveness, self.BROKER_TIMEOUT_MSEC, self.loop)
	# 	self.broker_timer.start()
	# 	if self.broker_aliveness == 0:
	# 		self.reconnect()
	# 	else:
	# 		self.ping(self.frontend)
	

	def do_work(self, task):
		return 'OK, work done'

	
	def setup(self):
		self.reset_aliveness()
		self.stream, self.socket = self.stream('frontend', zmq.DEALER, False, self.handle_broker_msg)
		self.socket.send_multipart(["", MsgCode.STATUS_READY])
		self.update_broker_timeout()
		ioloop.DelayedCallback(self.timed_ping, self.BROKER_TIMEOUT_MSEC, self.loop).start()



		
		
	
class JobQueueBroker(PingPongBroker):


	
	def setup(self):
		super(JobQueueBroker, self).setup()
		self.jobs = DQueue(item_type = Job)
		self.jobs_received = 0

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

	def handle_backend(self, package):
		if package.msg == MsgCode.JOB_COMPLETE:
			if package.encapsulated:
				### Forward result from worker to client
				self.say('Sending on frontend: {}'.format(package.encapsulated))
				package.encapsulated.send(self.frontend)





AddressManager.register_endpoint('market_frontend', 'tcp', 'localhost', 5562)
AddressManager.register_endpoint('market_backend', 'tcp', 'localhost', 5563)

if __name__ == '__main__':
	PingPongBroker(name = 'server', endpoints = {'backend' : 'market_backend'}).start()
	PingPongWorker(name = 'client', endpoints = {'frontend' : 'market_backend'}).start()

