
from workers import Auth
import zmq
from lib import *
from heapdict import heapdict
from brokers import Job
from traders import Trader
from datetime import datetime, timedelta
from bidict import bidict
import abc



class PingPongBroker(AgentProcess):
	WORKER_EXPIRE_SECONDS = 3

	def setup(self):
		self.frontend = self.context.socket(zmq.ROUTER)
		self.frontend.bind(AddressManager.get_bind_address(self.frontend_name))
		self.attach_handler(self.frontend, self.handle_frontend)

		self.backend = self.context.socket(zmq.ROUTER)
		self.backend.bind(AddressManager.get_bind_address(self.backend_name))
		self.attach_handler(self.backend, self.handle_backend)
		
		self.poller = zmq.Poller()
		self.poller.register(self.backend, zmq.POLLIN)
		self.poller.register(self.frontend, zmq.POLLIN)
		
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
		# self.say('Adding worker {}. Expires at {}'.format(worker_addr, expires))
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




if __name__ == '__main__':
	Trader('trader', None, 'market_frontend', verbose = True).start()	

	market_broker = PingPongBrokerNew('market_gateway', 'market_frontend', 'market_backend', verbose = True)
	market_broker.start()
	Auth('authenticator', 'market_backend', 'db_frontend', verbose = True).start()
	# broker = BrokerWithHandlers

	# a = AgentWithHandlers()
	# a.start()