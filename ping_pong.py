from uuid import uuid4
import zmq
from zmq.eventloop import ioloop, zmqstream
from new import Agent
from datetime import datetime, timedelta
from lib import AddressManager, MsgCode
from heapdict import heapdict

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
		self.simulate_overload(0)
		self.add_worker(worker_addr)
		self.backend_socket.send_multipart([worker_addr, "", MsgCode.PONG])


AddressManager.register_endpoint('market_frontend', 'tcp', 'localhost', 6001)
AddressManager.register_endpoint('market_backend', 'tcp', 'localhost', 6002)












if __name__ == '__main__':
	PingPongBroker(name = 'server', endpoints = {'backend' : 'market_backend'}).start()
	PingPongWorker(name = 'client', endpoints = {'frontend' : 'market_backend'}).start()
	# Server(name = 'server', endpoints = {'backend' : 'market_backend'}).start()
	# Client(name = 'client', endpoints = {'frontend' : 'market_backend'}).start()
