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
		
	

class Server(Agent):
	def handle_socket(self, msg):
		address, m = msg[0], msg[2]
		self.say(m)
		timer = ioloop.DelayedCallback(lambda: self.send_reply(address), 1000)
		self.workers[address] = (timer, m)
		timer.start()
		# self.socket.send_multipart([address, '', 'REQUEST OK'])

	def send_reply(self, address):
		reply = self.workers.get(address)[1]
		self.socket.send_multipart([address, '', '******' + reply])

	def setup(self):
		self.workers = {}
		self.stream, self.socket = self.stream('backend', zmq.ROUTER, True, self.handle_socket)


from time import sleep

class Client(Agent):
	i = 0

	def handle_socket(self, msg):
		self.say("{} ,".format(self.i) + "".join(msg))
		self.i += 1
		self.socket.send_multipart(["", 'NEW REQUEST FROM WORKER %s'%self.name])

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
		ioloop.DelayedCallback(self.reconnect, 30000, self.loop).start()
		self.stream, self.socket = self.stream('frontend', zmq.DEALER, False, self.handle_socket)
		self.socket.send_multipart(["", 'READY %s'%self.name])



from datetime import datetime, timedelta
from heapdict import heapdict

class PingPongBroker(Agent):
	WORKER_EXPIRE_SECONDS = 3

	def setup(self):
		self.backend_stream, self.backend_socket = self.stream('backend', zmq.ROUTER, True, self.handle_worker_msg)
		self.workers = heapdict()
		
	def expire_workers(self):
		if len(self.workers) > 0:
			now = datetime.now()
			while len(self.workers) > 0 and self.workers.peekitem()[1] < now:
				self.say('Expiring worker')
				self.workers.popitem()

	def add_worker(self, worker_addr):
		"""
		Store the worker address in the queue of ready workers
		"""
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
		self.simulate_overload()
		self.add_worker(worker_addr)
		self.backend_socket.send_multipart([worker_addr, "", MsgCode.PONG])

class PingPongWorker(Agent):
	"""
	Worker classed that is used with PingPongBroker. The worker and broker keep a PING/PONG dialogue
	to stay informed of whether the other party is still alive.
	"""

	i = 0

	PING_RETRIES = 3 ### How many times the worker will send out a PING and wait for a PONG
	BROKER_TIMEOUT_SECONDS = 1 

	def handle_broker_msg(self, msg):
		self.say("{} ,".format(self.i) + "".join(msg))
		self.i += 1
		task = msg[1]
		self.reset_aliveness()
		self.update_broker_timeout()

		if not task == MsgCode.PONG:
			result = self.do_work(task)
			self.stream.send_multipart(["", result])
			self.broker_timer.start()

	def update_broker_timeout(self):
		self._broker_timeout = datetime.now() + timedelta(seconds = self.BROKER_TIMEOUT_SECONDS)

	def timed_ping(self):
		"""
		This method is called ev
		"""
		if datetime.now() > self._broker_timeout:
			self._broker_aliveness -= 1
		if self._broker_aliveness == 1:
			self.reconnect()
		else:
			self.stream.send_multipart(["", MsgCode.PING])
			self.broker_timer = ioloop.DelayedCallback(self.timed_ping, self.BROKER_TIMEOUT_SECONDS * 1000, self.loop)
			self.broker_timer.start()

	def reset_aliveness(self):
		self._broker_aliveness = self.PING_RETRIES

	def reconnect(self):
		self.say('Reconnecting...')
		self.stream.flush()
		self.stream.close()
		del self.stream
		self.setup()
	
	def do_work(self, task):
		self.simulate_overload()
		return 'OK, work done'

	
	def setup(self):
		self.reset_aliveness()
		self.stream,s = self.stream('frontend', zmq.DEALER, False, self.handle_broker_msg)
		self.stream.send_multipart(["", MsgCode.STATUS_READY])
		self.update_broker_timeout()
		ioloop.DelayedCallback(self.timed_ping, self.BROKER_TIMEOUT_SECONDS, self.loop).start()


from transitions import Machine



class PPWorker(Agent):
	

	def EVENT_frontend_msg(self, msg):
		msg = msg[1]
		self.say(msg)
		self.state = 3
		if msg == MsgCode.PONG:
			self.action_reset_timer()
		else:
			self.action_stop_timer()
			self.action_do_work(msg)

	def event_timer_exp(self):
		self.state -= 1
		if self.state > 0:
			self.action_reset_timer()
			self.action_send_ping()
		else:
			self.reset_timer()
			self.action_reconnect()
			self.action_send_ready()



	def event_finish_work(self, result):
		self.frontend.send_multipart(['', result])
		self.action_reset_timer()
		self.state = 3

	

	def action_start_timer(self):
		self.timer = ioloop.DelayedCallback(self.event_timer_exp, 1000, self.loop)
		self.timer.start()

	def action_stop_timer(self):
		self.timer.stop()

	def action_reset_timer(self):
		self.action_stop_timer()
		self.action_start_timer()

	def action_connect_frontend(self):
		self.frontend,s = self.stream('frontend', zmq.DEALER, False, self.EVENT_frontend_msg)

	def action_reconnect(self):
		self.frontend.close()
		del self.frontend
		self.action_connect_frontend()


	def action_do_work(self, task):
		result = 'Work done: %s'%task
		self.event_finish_work(result)


	def action_send_ping(self):
		self.frontend.send_multipart(['', MsgCode.PING])

	def action_send_ready(self):
		self.frontend.send_multipart(['', MsgCode.STATUS_READY])

	
	def setup(self):
		self.state = 3
		self.action_connect_frontend()
		self.action_start_timer()



	# def recv_jon(self, msg):

	def broker_expired(self):
		if self.broker_aliveness <= 0: return True
		else: return False
		
		
	
# class JobQueueBroker(PingPongBroker):


	
# 	def setup(self):
# 		super(JobQueueBroker, self).setup()
# 		self.jobs = DQueue(item_type = Job)
# 		self.jobs_received = 0

# 	def send_job(self):
# 		worker_addr = self.workers.popitem()[0]
# 		job = self.jobs.get()
# 		client_p = Package(dest_addr = job.client)
# 		package = Package(dest_addr = worker_addr, msg = job.work, encapsulated = client_p)
# 		self.say('sending on backend: {}'.format(package))
# 		package.send(self.backend)

# 	def handle_frontend(self):
# 		package = Package.recv(self.frontend)
# 		self.say('On frontend: {}'.format(package))
# 		job = Job(client=package.sender_addr, work=package.msg)
# 		self.jobs_received += 1
# 		self.jobs.put(job)

# 	def handle_backend(self, package):
# 		if package.msg == MsgCode.JOB_COMPLETE:
# 			if package.encapsulated:
# 				### Forward result from worker to client
# 				self.say('Sending on frontend: {}'.format(package.encapsulated))
# 				package.encapsulated.send(self.frontend)





AddressManager.register_endpoint('market_frontend', 'tcp', 'localhost', 5562)
AddressManager.register_endpoint('market_backend', 'tcp', 'localhost', 5563)












if __name__ == '__main__':
	PingPongBroker(name = 'server', endpoints = {'backend' : 'market_backend'}).start()
	PPWorker(name = 'client', endpoints = {'frontend' : 'market_backend'}).start()
	# Server(name = 'server', endpoints = {'backend' : 'market_backend'}).start()
	# Client(name = 'client', endpoints = {'frontend' : 'market_backend'}).start()

